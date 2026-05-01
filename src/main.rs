use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use futures::{StreamExt, stream::FuturesUnordered, task::SpawnExt};
use pulsar::{
    Consumer, ConsumerOptions, ProducerOptions, Pulsar, SerializeMessage, SubType, TokioExecutor,
    compression::CompressionSnappy, consumer::InitialPosition, error::ProducerError,
};
use redis::AsyncCommands;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    enable_tracing();

    match std::env::var("TEST_CASE").as_ref().map(|v| v.as_str()) {
        Ok("PRODUCER") => run_producer().await,
        Ok("CONSUMER") => run_consumer().await,
        _ => {
            eprintln!("Run executable with TEST_CASE set to PRODUCER or CONSUMER");
            return;
        }
    }
}

async fn run_consumer() {
    let redis = get_redis().await;
    let pulsar = get_pulsar().await;

    let mut consumer: Consumer<String, TokioExecutor> = pulsar
        .consumer()
        .with_topics(&vec![TOPIC])
        .with_consumer_name("delivery-consumer")
        .with_subscription_type(SubType::Shared)
        .with_subscription("delivery-subscription")
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .with_unacked_message_resend_delay(Some(Duration::from_secs(60 * 30)))
        .build()
        .await
        .unwrap();

    println!("Running Consumer");

    let mut count: usize = 0;
    while let Some(Ok(msg)) = consumer.next().await {
        consumer.ack(&msg).await.unwrap();
        count += 1;
        if count % 10000 == 0 {
            println!(".");
        }
    }
}

const WORKER_COUNT: u64 = 50;
const PAUSE_BETWEEN_REDIS_CHECKS: u64 = 250;
const SEND_COUNT: u64 = 300;

async fn run_producer() {
    let redis = get_redis().await;

    let tasks = FuturesUnordered::new();
    for worker in 0..WORKER_COUNT {
        println!("Spawning Worker: {worker}");
        let redis = redis.clone();
        tasks
            .spawn(async move {
                spam_deliveries(redis).await;
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _: Vec<_> = tasks.collect().await;
}

async fn spam_deliveries(pool: Pool<RedisConnectionManager>) {
    let pulsar = get_pulsar().await;

    let mut producer = pulsar
        .producer()
        .with_name(format!("producer: {}", uuid::Uuid::new_v4()))
        .with_options(ProducerOptions {
            compression: Some(pulsar::compression::Compression::Snappy(
                CompressionSnappy::default(),
            )),
            ..Default::default()
        })
        .build_multi_topic();

    loop {
        let app_id = uuid::Uuid::new_v4();

        let sending = FuturesUnordered::new();
        for _ in 0..SEND_COUNT {
            loop {
                let notification_id = uuid::Uuid::new_v4();

                let send_future = match producer
                    .send_non_blocking(
                        TOPIC,
                        TestMessage {
                            app_id: app_id.clone(),
                            notification_id,
                        },
                    )
                    .await
                {
                    Ok(send_future) => send_future,
                    Err(pulsar::Error::Producer(e))
                        if matches!(
                            e,
                            ProducerError::Connection(pulsar::error::ConnectionError::SlowDown)
                        ) =>
                    {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                    Err(e) => {
                        println!(
                            "Error sending delivery non_blocking ({e:?}). Trying again in a second"
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                sending.push(send_future);
            }
        }
        for send in sending.collect::<Vec<_>>().await {
            if let Err(e) = send {
                println!("Error sending delivery non_blocking ({e:?})");
            }
        }
    }
}

struct TestMessage {
    app_id: uuid::Uuid,
    notification_id: uuid::Uuid,
}

impl SerializeMessage for TestMessage {
    fn serialize_message(input: Self) -> Result<pulsar::producer::Message, pulsar::Error> {
        let deliver_at_time = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Unix Epoch works")
                .as_millis() as i64
                + 1000,
        );

        Ok(pulsar::producer::Message {
            payload: format!(
                "{}:{}",
                input.app_id.to_string(),
                input.notification_id.to_string()
            )
            .as_bytes()
            .to_vec(),
            deliver_at_time,
            ..Default::default()
        })
    }
}

const TOPIC: &str = "persistent://example/delivery/notifications-enterprise-retries";

async fn get_redis() -> Pool<RedisConnectionManager> {
    let manager = bb8_redis::RedisConnectionManager::new("redis://redis:6379/0").unwrap();
    bb8::Pool::builder()
        .max_size(100)
        .connection_timeout(Duration::from_secs(10))
        .build(manager)
        .await
        .unwrap()
}

async fn get_pulsar() -> Pulsar<TokioExecutor> {
    Pulsar::builder("pulsar://broker:6650", TokioExecutor)
        .build()
        .await
        .unwrap()
}

fn enable_tracing() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
}
