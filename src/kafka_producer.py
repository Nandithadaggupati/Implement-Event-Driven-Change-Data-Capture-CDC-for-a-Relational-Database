import json
import logging
from confluent_kafka import Producer
from typing import Dict, Any

logger = logging.getLogger(__name__)

class CDCKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        
        # Configure the producer with retry capabilities
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'cdc-producer',
            'retries': 5,
            'retry.backoff.ms': 1000,
            'acks': 'all',  # Ensure data is committed
            'enable.idempotence': True # Ensure exactly-once semantics and ordered delivery
        }
        
        self.producer = Producer(conf)
        logger.info(f"Initialized Kafka Producer for topic: {self.topic}")

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def publish_event(self, event: Dict[str, Any]):
        """Publishes a CDC event to the configured Kafka topic."""
        try:
            event_json = json.dumps(event)
            # Produce the message, using primary key as the routing key
            key = str(event.get("primary_keys", {}))
            
            self.producer.produce(
                self.topic,
                key=key.encode('utf-8') if key else None,
                value=event_json.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # Serve delivery callback queue
            self.producer.poll(0)
            logger.info(f"Published {event.get('operation_type')} event for recording {event.get('primary_keys')} on table {event.get('table_name')}")
            
        except Exception as e:
            logger.error(f"Failed to publish event to Kafka: {e}")

    def flush(self, timeout=10.0):
        """Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered."""
        logger.info("Flushing Kafka Producer...")
        self.producer.flush(timeout)
