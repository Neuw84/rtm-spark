#!/usr/bin/env python3
"""
High-throughput Kafka event producer for the Spark 4.1 Real-time Mode demo.

Generates ~10,000 synthetic events per second and writes them as JSON to the
``events-input`` topic. Designed to run inside the ``jupyter`` container (which
ships ``kafka-python``), but works from anywhere that can reach the broker.

Usage (inside the jupyter container):
    python kafka_producer.py
    python kafka_producer.py --rate 10000 --duration 120

From the host (broker advertised as localhost:29092):
    python kafka_producer.py --bootstrap localhost:29092
"""
import argparse
import json
import os
import random
import time

from kafka import KafkaProducer

EVENT_TYPES = ["click", "view", "purchase", "add_to_cart", "logout", "login"]
COUNTRIES = ["US", "ES", "DE", "FR", "BR", "IN", "JP", "GB"]


def build_producer(bootstrap: str) -> KafkaProducer:
    # Throughput-oriented config: batch aggressively, compress, and let the
    # client coalesce records with a small linger window.
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=20,
        batch_size=64 * 1024,
        compression_type="lz4",
        buffer_memory=128 * 1024 * 1024,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def make_event(seq: int) -> dict:
    return {
        "event_id": seq,
        "user_id": random.randint(1, 100_000),
        "event_type": random.choice(EVENT_TYPES),
        "country": random.choice(COUNTRIES),
        "amount": round(random.uniform(0.0, 500.0), 2),
        "ts": time.time(),
    }


def run(bootstrap: str, topic: str, rate: int, duration: float) -> None:
    producer = build_producer(bootstrap)
    print(f"Producing ~{rate} events/sec to '{topic}' on {bootstrap} "
          f"for {duration:.0f}s (Ctrl-C to stop)...")

    # Pace the load in 100 ms windows so the average rate stays close to target
    # without busy-spinning on time.time() for every single record.
    window = 0.1
    per_window = max(1, int(rate * window))

    seq = 0
    sent = 0
    start = time.time()
    last_report = start
    try:
        while True:
            window_start = time.time()
            if duration > 0 and (window_start - start) >= duration:
                break

            for _ in range(per_window):
                evt = make_event(seq)
                # Key by user_id so events of the same user land on one partition.
                producer.send(topic, key=str(evt["user_id"]), value=evt)
                seq += 1
                sent += 1

            now = time.time()
            if now - last_report >= 2.0:
                elapsed = now - start
                print(f"  sent={sent:,}  avg_rate={sent / elapsed:,.0f}/s  "
                      f"elapsed={elapsed:,.0f}s")
                last_report = now

            # Sleep for the remainder of the window to hold the target rate.
            spent = time.time() - window_start
            if spent < window:
                time.sleep(window - spent)
    except KeyboardInterrupt:
        print("\nInterrupted, flushing...")
    finally:
        producer.flush()
        producer.close()
        elapsed = max(time.time() - start, 1e-9)
        print(f"Done. total_sent={sent:,}  avg_rate={sent / elapsed:,.0f}/s")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark 4.1 Real-time Mode demo producer")
    p.add_argument("--bootstrap",
                   default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                   help="Kafka bootstrap servers (default: env KAFKA_BOOTSTRAP or kafka:9092)")
    p.add_argument("--topic", default="events-input", help="Target topic")
    p.add_argument("--rate", type=int, default=10_000, help="Target events per second")
    p.add_argument("--duration", type=float, default=0,
                   help="Seconds to run; 0 means run until interrupted")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.bootstrap, args.topic, args.rate, args.duration)
