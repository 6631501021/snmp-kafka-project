import json
import os
import subprocess
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# Router / SNMP configuration
ROUTER_IP = os.getenv("SNMP_ROUTER_IP", "192.168.126.132")
COMMUNITY = os.environ["SNMP_COMMUNITY"]
IF_INDEX = 3  # Fa1/1, discovered with SNMP ifName walk
INTERFACE = "Fa1/1"
POLL_INTERVAL = 5  # seconds

# Kafka configuration: override with env vars if needed
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "snmp")

# SNMPv2c 32-bit cumulative octet counters
IN_OID = f"1.3.6.1.2.1.2.2.1.10.{IF_INDEX}"   # ifInOctets
OUT_OID = f"1.3.6.1.2.1.2.2.1.16.{IF_INDEX}"  # ifOutOctets
COUNTER_MODULUS = 2**32

def get_counters():
    """Read both counters in one SNMP request. Return (in_octets, out_octets)."""
    cmd = [
        "snmpget", "-v2c", "-c", COMMUNITY,
        "-t", "2", "-r", "1", "-Oqv",
        ROUTER_IP, IN_OID, OUT_OID,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True,
                            check=True, timeout=8)
    lines = result.stdout.strip().splitlines()
    if len(lines) != 2:
        raise ValueError(f"Expected 2 counters, got: {result.stdout!r}")
    try:
        inbound, outbound = (int(line.strip()) for line in lines)
    except ValueError as exc:
        raise ValueError(f"Invalid SNMP counter values: {lines!r}") from exc
    if not (0 <= inbound < COUNTER_MODULUS and 0 <= outbound < COUNTER_MODULUS):
        raise ValueError(f"Out-of-range 32-bit counters: {lines!r}")
    return inbound, outbound


def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda msg: json.dumps(msg).encode("utf-8"),
            acks="all",
            retries=3,
            max_block_ms=10000,
        )
    except KafkaTimeoutError as exc:
        raise SystemExit(
            f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {exc}"
        ) from exc

    print(f"SNMP: {ROUTER_IP} {INTERFACE} (ifIndex={IF_INDEX})")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC}")
    print(f"Polling every {POLL_INTERVAL}s. Ctrl+C to stop.\n")

    previous = None  # (in_octets, out_octets, monotonic_time)
    try:
        while True:
            started = time.monotonic()
            try:
                inbound, outbound = get_counters()
                measured_at = time.monotonic()
                timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

                interval = None
                in_delta = None
                out_delta = None
                in_mbps = None
                out_mbps = None

                if previous is not None:
                    prev_in, prev_out, prev_time = previous
                    interval = measured_at - prev_time
                    in_delta = (inbound - prev_in) % COUNTER_MODULUS
                    out_delta = (outbound - prev_out) % COUNTER_MODULUS
                    in_mbps = in_delta * 8 / interval / 1_000_000
                    out_mbps = out_delta * 8 / interval / 1_000_000

                message = {
                    "timestamp": timestamp,
                    "router_ip": ROUTER_IP,
                    "interface": INTERFACE,
                    "if_index": IF_INDEX,
                    "in_octets": inbound,
                    "out_octets": outbound,
                    "interval_seconds": round(interval, 3) if interval is not None else None,
                    "in_bytes_delta": in_delta,
                    "out_bytes_delta": out_delta,
                    "in_mbps": round(in_mbps, 4) if in_mbps is not None else None,
                    "out_mbps": round(out_mbps, 4) if out_mbps is not None else None,
                }

                # Wait for Kafka's delivery acknowledgment before marking a
                # sample published and updating the previous counter snapshot.
                metadata = producer.send(KAFKA_TOPIC, value=message).get(timeout=10)
                print(
                    f"[{timestamp}] IN={inbound:,} OUT={outbound:,} | "
                    f"IN Mbps={in_mbps:.4f} OUT Mbps={out_mbps:.4f} | "
                    f"Kafka partition={metadata.partition}, offset={metadata.offset}"
                    if in_mbps is not None else
                    f"[{timestamp}] IN={inbound:,} OUT={outbound:,} | "
                    f"first sample | Kafka partition={metadata.partition}, offset={metadata.offset}",
                    flush=True,
                )
                previous = (inbound, outbound, measured_at)

            except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
                    FileNotFoundError, ValueError, KafkaError) as exc:
                print(f"ERROR (sample not published): {exc}", flush=True)

            time.sleep(max(0.0, POLL_INTERVAL - (time.monotonic() - started)))

    except KeyboardInterrupt:
        print("\nCollector stopped.")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
