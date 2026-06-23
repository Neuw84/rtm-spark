# rtm-spark — Apache Spark 4.1 Real-time Mode demo

A self-contained, runnable demo of **Real-time Mode (RTM)**, the new streaming
trigger introduced in [Apache Spark 4.1.0](https://spark.apache.org/releases/spark-release-4.1.0.html).
RTM launches long-running tasks (one per input partition) so records flow
`source -> transform -> sink` continuously, targeting millisecond-scale latency
instead of the ~100 ms micro-batch floor — while keeping the same
`readStream` / `writeStream` DataFrame API and the same fault-tolerance
guarantees.

The stack reads a ~10,000 events/sec Kafka stream, applies a **stateless**
transformation, and writes it back to Kafka in Real-time Mode, all from a single
notebook.

> For the full write-up and design rationale, see [`rtm-blog.md`](./rtm-blog.md).

## What's in the box

- **Apache Spark 4.1.2** standalone cluster — master + two workers (4 cores each,
  so **8 worker cores** total) + history server, built on Ubuntu 24.04 / Python 3.12.
- **Apache Kafka 4.1.0**, single node, **KRaft** mode (no ZooKeeper). Reachable as
  `kafka:9092` inside the Docker network and `localhost:29092` from your host.
- **JupyterLab** as the Spark driver, running natively on your host architecture
  (avoids an ipykernel deadlock under amd64 emulation on Apple Silicon).
- A **`kafka-init`** one-shot that creates the `events-input` and `events-output`
  topics with 6 partitions each.

The Kafka Structured Streaming connector jars are baked into the Spark and Jupyter
images, so there's no `spark.jars.packages` / Ivy resolution at session start.

## Repository layout

| Path | Purpose |
| --- | --- |
| `docker-compose.yaml` | The full stack: Spark master + 2 workers + history server + JupyterLab + Kafka + topic init. |
| `DockerfileSpark` | Spark 4.1.2 image (Hadoop 3.4.2 client, AWS SDK v2/S3A, Comet, Kafka connector jars). |
| `DockerfileJupyter` | JupyterLab driver image, same Spark + jars layered on the Jupyter base. |
| `realtime_streaming.ipynb` | The demo notebook: produce -> RTM transform -> sink -> verify -> measure latency. |
| `kafka_producer.py` | Standalone ~10k events/sec producer (twin of the notebook's inline producer). |
| `scripts/apache-dl.sh` | Helper that downloads Apache dist tarballs from the fastest available mirror. |
| `rtm-blog.md` | Long-form blog post explaining Real-time Mode and this demo. |

## Quick start

```bash
docker compose up --build
```

Then open JupyterLab at **http://localhost:8888** (the token is disabled for local
convenience — secure it before using this anywhere shared) and run
`realtime_streaming.ipynb` top to bottom. The notebook and producer script are
bind-mounted into the Jupyter working directory, so they're already there.

### Service endpoints

| URL | Service |
| --- | --- |
| http://localhost:8888 | JupyterLab (the Spark driver) |
| http://localhost:8080 | Spark master UI |
| http://localhost:8081 / :8082 | Spark worker UIs |
| http://localhost:4040 | Spark driver UI (while a job runs) |
| http://localhost:18080 | Spark history server |
| localhost:29092 | Kafka bootstrap (from the host) |

## What the notebook does

1. **Produce** ~10,000 synthetic JSON events/sec into `events-input`, keyed by
   `user_id`.
2. **Read** the stream and apply a stateless transform (`from_json`, `filter`,
   `withColumn`) — keep revenue-bearing events, add a tax-inclusive amount,
   normalize the country code, stamp a processing time.
3. **Sink** the result back to Kafka (`events-output`) using Real-time Mode.
4. **Verify** by consuming a few output records, then **measure** end-to-end
   latency with timestamped probe events.

### Enabling Real-time Mode on Spark 4.1.2

On the stable 4.1.2 release the PySpark `trigger()` wrapper doesn't expose
`realTime`, so the notebook reaches the JVM trigger through a small py4j bridge:

```python
_jvm = spark._sc._jvm
_real_time_trigger = _jvm.org.apache.spark.sql.streaming.Trigger.RealTime("10 seconds")
writer._jwrite.trigger(_real_time_trigger)
query = writer.start()
```

If you install the `pyspark==4.2.0.dev5` preview instead, Python gets the trigger
natively and you can write `.trigger(realTime="10 seconds")` directly.

> The `"10 seconds"` is the **checkpoint cadence** (minimum 5s), **not** a latency
> target. Records still flow through the long-running tasks within milliseconds.

## Running the producer standalone

Inside the `jupyter` container (or anywhere that can reach the broker):

```bash
python kafka_producer.py                       # ~10k events/sec until interrupted
python kafka_producer.py --rate 10000 --duration 120
python kafka_producer.py --bootstrap localhost:29092   # from the host
```

## Real-time Mode constraints (Spark 4.1.x)

- **Stateless / map-like queries only** — no aggregations, windows, joins, or
  dedup yet (those are the Spark 4.3 target).
- **Output mode must be `update`.**
- **A `checkpointLocation` is required.**
- **Adaptive Query Execution (AQE) is not supported** — the demo sets
  `spark.sql.adaptive.enabled=false`.
- **One core per input partition, held continuously.** Keep partitions ≤ worker
  cores (6 partitions ≤ 8 cores here) or some readers never get scheduled.

## Notes

- The local latency numbers are a *relative* demonstration, not an absolute
  benchmark — everything runs on one host, with JSON on the hot path and (on
  Apple Silicon) some amd64 emulation for the workers. See the blog for details.
- This stack is for local experimentation. Kafka uses `PLAINTEXT` and the Jupyter
  token is disabled; add authentication and TLS before exposing it anywhere shared.

## Requirements

- Docker with Docker Compose v2 (`docker compose`).
- Enough headroom for the cluster (two workers configured at 4 cores / 4 GB each).
