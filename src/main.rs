use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use futures::{StreamExt, stream::FuturesUnordered, task::SpawnExt};
use pulsar::{
    Consumer, ConsumerOptions, ProducerOptions, Pulsar, SerializeMessage, SubType, TokioExecutor,
    compression::CompressionSnappy, consumer::InitialPosition, error::ProducerError,
};
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

/// How long without receiving a message before we consider the dispatcher stuck.
const STALENESS_THRESHOLD_SECS: u64 = 30;

async fn run_consumer() {
    let _redis = get_redis().await;
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

    println!("Running Consumer — staleness threshold: {}s", STALENESS_THRESHOLD_SECS);

    let total_count = Arc::new(AtomicU64::new(0));
    let last_received_epoch = Arc::new(AtomicU64::new(epoch_secs()));
    let stuck_detected = Arc::new(AtomicBool::new(false));

    // Staleness watchdog — runs in background
    {
        let last = last_received_epoch.clone();
        let total = total_count.clone();
        let stuck = stuck_detected.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let gap = epoch_secs() - last.load(Ordering::Relaxed);
                let count = total.load(Ordering::Relaxed);
                if gap >= STALENESS_THRESHOLD_SECS {
                    eprintln!(
                        "🚨 STALE DISPATCHER DETECTED — no messages for {}s (total received: {}). \
                         Check broker logs for NoSuchElementException!",
                        gap, count,
                    );
                    stuck.store(true, Ordering::Relaxed);
                } else if stuck.load(Ordering::Relaxed) {
                    eprintln!(
                        "✅ Dispatcher resumed after stall (total received: {})",
                        count,
                    );
                    stuck.store(false, Ordering::Relaxed);
                }
            }
        });
    }

    let mut count: u64 = 0;
    while let Some(Ok(msg)) = consumer.next().await {
        consumer.ack(&msg).await.unwrap();
        count += 1;
        total_count.store(count, Ordering::Relaxed);
        last_received_epoch.store(epoch_secs(), Ordering::Relaxed);
        if count % 10000 == 0 {
            println!("Consumed {} messages", count);
        }
    }
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

const WORKER_COUNT: u64 = 50;
/// Messages per batch per worker. All messages in a batch share the same deliver_at_time
/// to maximize the chance of landing in the same TreeMap bucket in the broker's
/// InMemoryDelayedDeliveryTracker.
const BATCH_SIZE: u64 = 300;
/// How far in the future to set deliver_at_time (milliseconds).
/// Shorter = messages become ready faster = getScheduledMessages drains faster = more race opportunities.
const DELAY_MS: i64 = 500;

async fn run_producer() {
    let _redis = get_redis().await;

    let tasks = FuturesUnordered::new();
    for worker in 0..WORKER_COUNT {
        println!("Spawning Worker: {worker}");
        tasks
            .spawn(async move {
                spam_deliveries(worker).await;
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _: Vec<_> = tasks.collect().await;
}

async fn spam_deliveries(worker_id: u64) {
    let pulsar = get_pulsar().await;

    let mut producer = pulsar
        .producer()
        .with_name(format!("producer-{}-{}", worker_id, uuid::Uuid::new_v4()))
        .with_options(ProducerOptions {
            compression: Some(pulsar::compression::Compression::Snappy(
                CompressionSnappy::default(),
            )),
            ..Default::default()
        })
        .build_multi_topic();

    let mut batch_count: u64 = 0;
    loop {
        // All messages in this batch get the EXACT same deliver_at_time.
        // This concentrates them into one TreeMap bucket on the broker,
        // so when getScheduledMessages drains that bucket it empties the map in one shot.
        let deliver_at = epoch_millis() + DELAY_MS;

        let sending = FuturesUnordered::new();
        for _ in 0..BATCH_SIZE {
            loop {
                let notification_id = uuid::Uuid::new_v4();

                let send_future = match producer
                    .send_non_blocking(
                        TOPIC,
                        TestMessage {
                            notification_id,
                            deliver_at,
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
                            "Worker {worker_id}: Error sending ({e:?}). Retrying in 1s"
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                sending.push(send_future);
                break; // <-- FIX: break the retry loop after successful enqueue
            }
        }
        // Wait for all sends in the batch to confirm
        let mut errors = 0;
        for send in sending.collect::<Vec<_>>().await {
            if let Err(e) = send {
                errors += 1;
                if errors <= 3 {
                    println!("Worker {worker_id}: Send error ({e:?})");
                }
            }
        }

        batch_count += 1;
        if batch_count % 10 == 0 {
            println!(
                "Worker {worker_id}: sent {} batches ({} messages total, {} errors last batch)",
                batch_count,
                batch_count * BATCH_SIZE,
                errors,
            );
        }
    }
}

fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct TestMessage {
    notification_id: uuid::Uuid,
    /// All messages in a batch share this exact timestamp to land in the same
    /// broker TreeMap bucket, maximizing the race window in getScheduledMessages.
    deliver_at: i64,
}

impl SerializeMessage for TestMessage {
    fn serialize_message(input: Self) -> Result<pulsar::producer::Message, pulsar::Error> {
        Ok(pulsar::producer::Message {
            payload: input.notification_id.to_string().as_bytes().to_vec(),
            deliver_at_time: Some(input.deliver_at),
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
