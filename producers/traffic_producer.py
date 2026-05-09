import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


JUNCTIONS = [
    {"sensor_id": "COL-001", "junction_name": "Borella"},
    {"sensor_id": "COL-002", "junction_name": "Rajagiriya"},
    {"sensor_id": "COL-003", "junction_name": "Pettah"},
    {"sensor_id": "COL-004", "junction_name": "Nugegoda"},
]

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = "traffic-data"


def create_producer():
    """Create a Kafka producer that writes JSON messages."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def generate_sensor_event(junction):
    """
    Generate one traffic event.
    A small percentage of events are intentionally critical with very low speed.
    """
    is_critical = random.random() < 0.2

    if is_critical:
        avg_speed = round(random.uniform(3.0, 9.5), 2)
        vehicle_count = random.randint(70, 140)
    else:
        avg_speed = round(random.uniform(18.0, 55.0), 2)
        vehicle_count = random.randint(15, 100)

    return {
        "sensor_id": junction["sensor_id"],
        "junction_name": junction["junction_name"],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": avg_speed,
    }


def main():
    producer = create_producer()
    print(f"[Producer] Sending traffic events to Kafka topic '{TOPIC_NAME}'...")

    try:
        while True:
            for junction in JUNCTIONS:
                event = generate_sensor_event(junction)
                producer.send(TOPIC_NAME, value=event)

                traffic_status = "CRITICAL" if event["avg_speed"] < 10 else "NORMAL"
                print(
                    f"[Producer] {traffic_status} | "
                    f"{event['junction_name']} | "
                    f"vehicles={event['vehicle_count']} | "
                    f"speed={event['avg_speed']} km/h"
                )

            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        print("[Producer] Stopping producer...")
    finally:
        producer.close()
        print("[Producer] Kafka producer closed.")


if __name__ == "__main__":
    main()
