	
#!/usr/bin/env python3
import json
import os

from kafka import KafkaConsumer
from kafka.errors import KafkaError

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "snmp")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "snmp-worker")


def show_traffic(record):
    data = json.loads(record.value.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object")

    in_bytes = int(data["in_octets"])
    out_bytes = int(data["out_octets"])
    in_rate = data.get("in_mbps")
    out_rate = data.get("out_mbps")

    print(f"\n[{data.get('timestamp', 'unknown')}] offset={record.offset}")
    print(f"Router: {data.get('router_ip', 'unknown')}")
    print(f"Interface: {data.get('interface', 'unknown')} (ifIndex={data.get('if_index', '?')})")
    print(f"IN:  {in_bytes:,} bytes | {float(in_rate):.4f} Mbps" if in_rate is not None
          else f"IN:  {in_bytes:,} bytes | rate pending")
    print(f"OUT: {out_bytes:,} bytes | {float(out_rate):.4f} Mbps" if out_rate is not None
          else f"OUT: {out_bytes:,} bytes | rate pending", flush=True)


def main():
    consumer = None
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKERS.split(","),
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print(f"Connected: {BROKERS} | topic={TOPIC} | group={GROUP_ID}", flush=True)

        for record in consumer:
            try:
                show_traffic(record)
            except (ValueError, TypeError, KeyError, UnicodeDecodeError) as exc:
                print(f"Skipping invalid message at offset {record.offset}: {exc}", flush=True)

    except KeyboardInterrupt:
        print("\nWorker stopped.")
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()

