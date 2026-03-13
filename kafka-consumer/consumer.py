import os
import json
import logging
import time
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cdc_events")

def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'cdc-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to topic '{KAFKA_TOPIC}'. Waiting for messages...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.debug(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n')
                elif msg.error():
                    # Instead of crashing, just log the error and wait
                    logger.warning(f"Kafka error: {msg.error()}")
                    time.sleep(1)
            else:
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    
                    logger.info("="*50)
                    logger.info(f"Received CDC Event ({event.get('operation_type')}) for table: {event.get('table_name')}")
                    logger.info(json.dumps(event, indent=2))
                    logger.info("="*50)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    logger.error(f"Raw message: {msg.value()}")
                    
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == '__main__':
    main()
