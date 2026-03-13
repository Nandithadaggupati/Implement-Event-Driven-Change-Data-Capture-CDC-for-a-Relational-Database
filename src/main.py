import os
import time
import signal
import sys
import logging
from dotenv import load_dotenv

from cdc_processor import CDCProcessor
from kafka_producer import CDCKafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_HOST = os.environ.get("CDC_DB_HOST", "mysql")
DB_PORT = int(os.environ.get("CDC_DB_PORT", 3306))
DB_USER = os.environ.get("CDC_DB_USER", "root")
DB_PASSWORD = os.environ.get("CDC_DB_PASSWORD", "root_password")
DB_NAME = os.environ.get("CDC_DB_NAME", "cdc_db")
TABLE_NAME = os.environ.get("CDC_TABLE_NAME", "products")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cdc_events")
POLLING_INTERVAL = int(os.environ.get("POLLING_INTERVAL", 5))

class CDCApplication:
    def __init__(self):
        self.running = True
        self.processor = None
        self.producer = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Gracefully stopping CDC service...")
        self.running = False

    def start(self):
        logger.info("Starting CDC Application...")
        
        # Initialize components
        self.producer = CDCKafkaProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
        self.processor = CDCProcessor(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, TABLE_NAME)

        logger.info(f"Beginning poll loop (Interval: {POLLING_INTERVAL}s)")
        try:
            while self.running:
                events = self.processor.poll_changes()
                
                if events:
                    logger.info(f"Detected {len(events)} changes.")
                    for event in events:
                        self.producer.publish_event(event)
                
                # Sleep in small increments to allow responsive shutdown
                for _ in range(POLLING_INTERVAL * 10):
                    if not self.running:
                        break
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self):
        if self.producer:
            self.producer.flush()
        if self.processor:
            self.processor.close()
        logger.info("CDC Service stopped.")


if __name__ == "__main__":
    app = CDCApplication()
    app.start()
