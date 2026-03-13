# Event-Driven Change Data Capture (CDC) Pipeline

A robust, event-driven data ingestion service that captures changes from a source MySQL database and publishes these changes as structured events to Apache Kafka. 

## Architecture

This service periodically polls a MySQL database for changes (INSERTs, UPDATEs, DELETEs) and publishes standardized JSON events to a Kafka topic (`cdc_events`). A simple Kafka consumer reads the topic and logs the events.
The whole system is containerized with Docker Compose.

- **Poller Mechanics**: 
  - Tracks the `last_updated` timestamp of row changes to detect INSERTS and UPDATES.
  - Maintains an in-memory cache of previously seen IDs to accurately detect DELETE operations.
  - Standardizes the database change into an Event schema containing the table name, operation type, event timestamp, and the old/new payloads.

### Event Schema Format

```json
{
  "event_id": "uuid-v4-string",
  "timestamp": "2023-10-27T10:30:00Z",
  "table_name": "products",
  "operation_type": "INSERT", // Or "UPDATE", "DELETE"
  "primary_keys": {"id": 123},
  "payload": {
    "old_data": null, // Present for UPDATE/DELETE
    "new_data": {     // Present for INSERT/UPDATE
         "id": 123,
         "name": "Laptop",
         "price": 1200.0,
         "stock": 50,
         "last_updated": "2023-10-27T10:30:00"
    }
  }
}
```

## Error Handling & Retries

- **Kafka Producer Retries**: The `CDCKafkaProducer` wrapper utilizes `confluent_kafka` configuration features (`retries`, `retry.backoff.ms`) to resend failed messages to Kafka if the broker is unreachable. It also enables idempotence for exactly-once semantics.
- **Database Resilience**: If the database goes down, the polling loop catches MySQL Connection errors and attempts to ping/reconnect on the next cycle instead of crashing the main application thread.

---

## Setup & Running

**Prerequisites:** Docker, Docker Compose, Git.

1. Clone the repository and navigate to the `cdc-pipeline` directory.
2. Initialize environment variables:
   ```bash
   cp .env.example .env
   ```
3. Boot the environment using Docker Compose:
   ```bash
   docker-compose up --build -d
   ```
   This command starts Zookeeper, Kafka, MySQL, the CDC service, and the Kafka Consumer service.

4. Check the status of the services:
   ```bash
   docker-compose ps
   ```

## Simulating Changes & Verification

1. Attach to the logs of the Kafka consumer to view events arriving in real-time:
   ```bash
   docker logs -f cdc-pipeline-kafka-consumer-1
   ```

2. Open a new terminal window to interact with the MySQL database:
   ```bash
   # Enter the MySQL container (password is 'root_password')
   docker exec -it cdc-pipeline-mysql-1 mysql -u root -p cdc_db
   ```
3. Execute SQL statements to trigger changes:
   ```sql
   -- INSERT a new record
   INSERT INTO products (name, description, price, stock) VALUES ('Monitor', '4K Display', 300.00, 15);

   -- UPDATE an existing record
   UPDATE products SET price = 250.00 WHERE name = 'Monitor';

   -- DELETE a record
   DELETE FROM products WHERE name = 'Laptop';
   ```

4. Go back to your first terminal window tailing the `kafka-consumer` logs. You will see the structured CDC events printed for each database operation.

## Running Tests

To run the event transformation logic unit tests:

```bash
# Requires pytest
pip install pytest mysql-connector-python confluent-kafka
export PYTHONPATH=./src
pytest tests/
```
