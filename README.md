# Cisco SNMP Traffic Monitoring with Apache Kafka

A Python-based network monitoring project that collects
Cisco router interface traffic via SNMP and streams
the measurements through Apache Kafka.

## Architecture

Cisco Router (SNMPv2c)
        |
        v
collector.py (Kafka Producer)
        |
        v
Apache Kafka (topic: snmp)
        |
        v
worker.py (Kafka Consumer)

## Technologies

- Python 3.12
- kafka-python 3.0.11
- Cisco IOS / GNS3
- SNMPv2c
- Apache Kafka 3.9.1
- Docker Compose
- Kafka UI

## Installation

sudo apt install python3-venv python3-pip snmp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Configuration

cp .env.example .env

Edit .env to configure your router and Kafka broker.

Load environment variables:

set -a
source .env
set +a

## Start Kafka

docker compose up -d

Create topic if it does not exist:

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists --topic snmp \
  --partitions 1 --replication-factor 1

Do not start a second Kafka stack if an existing
broker is already using ports 9092 and 8085.

## Run

Terminal 1:
python collector.py

Terminal 2:
python worker.py

## Monitoring

Lab router: 192.168.126.132
Interface: FastEthernet1/1
Interface index: 3
Polling interval: 5 seconds

The collector publishes cumulative inbound and
outbound byte counters and calculated Mbps rates.

## Notes

This is a local lab demonstration.
SNMPv2c and Kafka PLAINTEXT are not encrypted.
For production, use secure authentication and
transport protocols.

The 32-bit SNMP counters may wrap or reset.
Additional validation is required for reliable
production traffic-rate monitoring.
