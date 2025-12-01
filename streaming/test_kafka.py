"""
Simple Kafka Test Script
========================
Tests basic Kafka connectivity without Snowflake dependencies.

Run this to verify Kafka is working before running the full pipeline.
"""

import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TEST_TOPIC = "test-topic"


def test_producer():
    """Test sending messages to Kafka."""
    print("🔌 Creating Kafka producer...")

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        print("✅ Producer connected!")

        # Send test messages
        print("\n📤 Sending test messages...")
        for i in range(5):
            message = {
                "id": i,
                "message": f"Test message {i}",
                "timestamp": time.time(),
            }
            future = producer.send(TEST_TOPIC, value=message)
            try:
                record = future.get(timeout=10)
                print(
                    f"   Sent message {i} to {record.topic} partition {record.partition} offset {record.offset}"
                )
            except KafkaError as e:
                print(f"   ❌ Failed to send message {i}: {e}")

        producer.flush()
        producer.close()
        print("\n✅ Producer test complete!")
        return True

    except Exception as e:
        print(f"❌ Producer error: {e}")
        return False


def test_consumer():
    """Test consuming messages from Kafka."""
    print("\n🔌 Creating Kafka consumer...")

    try:
        consumer = KafkaConsumer(
            TEST_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="test-consumer-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,  # 5 second timeout
        )
        print("✅ Consumer connected!")

        # Consume messages
        print("\n📥 Receiving messages (5 second timeout)...")
        message_count = 0
        for message in consumer:
            print(f"   Received: {message.value}")
            message_count += 1

        consumer.close()
        print(f"\n✅ Consumer test complete! Received {message_count} messages")
        return True

    except Exception as e:
        print(f"❌ Consumer error: {e}")
        return False


def main():
    print("=" * 60)
    print("KAFKA CONNECTIVITY TEST")
    print("=" * 60)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Test topic: {TEST_TOPIC}")
    print("=" * 60)

    # Test producer
    producer_ok = test_producer()

    if producer_ok:
        # Small delay to ensure messages are available
        time.sleep(1)

        # Test consumer
        consumer_ok = test_consumer()
    else:
        consumer_ok = False

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Producer: {'✅ PASS' if producer_ok else '❌ FAIL'}")
    print(f"Consumer: {'✅ PASS' if consumer_ok else '❌ FAIL'}")

    if producer_ok and consumer_ok:
        print("\n🎉 Kafka is working correctly!")
        print("\nNext steps:")
        print("  1. Run the pitch producer: python streaming/kafka_pitch_producer.py")
        print("  2. Run the Spark consumer: python streaming/spark_kafka_consumer.py")
    else:
        print("\n⚠️  Kafka connectivity issues detected.")
        print("\nTroubleshooting:")
        print("  1. Make sure Kafka is running:")
        print("     cd streaming && docker-compose up -d")
        print("  2. Check container status:")
        print("     docker ps")
        print("  3. Check Kafka logs:")
        print("     docker logs streaming-kafka-1")


if __name__ == "__main__":
    main()
