# Steps to reproduce

- Checkout this repo
- cd to it
- `docker compose up -d`
- Get a console on broker-1 and copy & paste the three commands in script/setup_partitions to it (one time setup)
- In one tab run:
    - `docker compose exec code bash`
    - `apt-get update && apt-get install -y protobuf-compiler`
    - `TEST_CASE=CONSUMER cargo run --release.`
    - Wait for it to start
- In another tab run:
    - `docker compose exec code bash`
    - `TEST_CASE=PRODUCER cargo run --release`
- Wait three minutes
- docker compose logs broker-1 broker-2 broker-3 | grep -i NoSuchElementException

If successful, you will text like:
```
broker-1-1  | java.util.NoSuchElementException: null
broker-1-1  | java.util.NoSuchElementException: null
broker-3-1  | java.util.NoSuchElementException: null
broker-3-1  | java.util.NoSuchElementException: null
```
