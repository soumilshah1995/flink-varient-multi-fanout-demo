#!/usr/bin/env python3
"""
Order event producer for Dynamic Iceberg Sink Lab 2.
Produces schemaless JSON for VARIANT columns - no schema needed!
"""
import json
import time
import os
from kafka import KafkaProducer

# Use broker:9092 from inside Docker, localhost:29092 from host
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "events_stream"
NUM_EVENTS = 100  # 10 per site

SITES = ["siteA", "siteB", "siteC"]


def generate_order(site_id: str, event_num: int) -> dict:
    """Generate order with schemaless JSON payload for VARIANT."""
    return {
        "event_type": "order",
        "site_id": site_id,
        "event_id": f"order-{site_id}-{event_num:03d}",
        # Raw JSON string - no schema needed! This is the power of VARIANT
        "payload": json.dumps({
            "order_details": {
                "order_number": f"ORD-{event_num:05d}",
                "customer": {
                    "id": f"CUST-{event_num % 100}",
                    "name": f"Customer {event_num}",
                    "tier": ["gold", "silver", "bronze"][event_num % 3]
                },
                "items": [
                    {
                        "sku": f"SKU-{i}",
                        "quantity": (event_num + i) % 10 + 1,
                        "price": round(19.99 + i * 10, 2),
                        "metadata": {
                            "category": ["electronics", "books", "clothing"][i % 3],
                            "tags": [f"tag{j}" for j in range(i % 3 + 1)]
                        }
                    }
                    for i in range(event_num % 3 + 1)
                ],
                "shipping": {
                    "address": {
                        "street": f"{event_num} Main St",
                        "city": ["NYC", "SF", "LA"][event_num % 3],
                        "zip": f"{10000 + event_num % 90000}"
                    },
                    "method": ["standard", "express", "overnight"][event_num % 3]
                },
                "payment": {
                    "method": "credit_card",
                    "last4": f"{1000 + event_num % 9000}",
                    "amount": round(99.99 + event_num * 5.5, 2)
                }
            },
            "metadata": {
                "source": "web",
                "session_id": f"sess-{event_num % 50}",
                "user_agent": "Mozilla/5.0",
                "ip": f"192.168.{event_num % 256}.{event_num % 256}"
            }
        }),
        "ts": f"{time.time()}"
    }


def produce_events():
    """Produce order events with schemaless JSON to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"🚀 Producing {NUM_EVENTS} order events with schemaless JSON to {TOPIC}...")
    print(f"   Sites: {SITES}")
    print(f"   VARIANT: No schema required!")
    print(f"   Dynamic Sink will route to: event_{{site_id}} tables")
    print("-" * 70)

    event_counts = {}

    for i in range(NUM_EVENTS):
        site_id = SITES[i % len(SITES)]
        
        event = generate_order(site_id, i)
        producer.send(TOPIC, value=event)
        
        # Track counts
        table_name = f"event_{site_id}"
        event_counts[table_name] = event_counts.get(table_name, 0) + 1
        
        print(f"✓ [{i+1:02d}] order + {site_id:6s} → {table_name:16s} (schemaless JSON)")
        time.sleep(0.1)

    producer.flush()
    producer.close()

    print("-" * 70)
    print(f"✅ Produced {NUM_EVENTS} events:")
    for table_name, count in sorted(event_counts.items()):
        print(f"   {table_name:16s}: {count} events")
    print(f"\n💡 Check Flink UI - VARIANT currently has Dynamic Sink limitation")
    print(f"   Workaround: Using STRING in generator, VARIANT properties in table")


if __name__ == "__main__":
    produce_events()
