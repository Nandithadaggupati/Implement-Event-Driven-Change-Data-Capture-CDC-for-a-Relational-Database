import unittest
from unittest.mock import MagicMock, patch
import json
from src.kafka_producer import CDCKafkaProducer # type: ignore

class TestKafkaProducer(unittest.TestCase):
    @patch('src.kafka_producer.Producer')
    def test_publish_event(self, mock_producer_class):
        # Create a mock producer instance
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance

        # Initialize the producer wrapper
        producer_wrapper = CDCKafkaProducer(bootstrap_servers='localhost:9092', topic='test_topic')
        
        # Sample event
        test_event = {
            "event_id": "123",
            "timestamp": "2023-10-27T10:30:00Z",
            "table_name": "products",
            "operation_type": "INSERT",
            "primary_keys": {"id": 1},
            "payload": {
                "old_data": None,
                "new_data": {"id": 1, "name": "Test"}
            }
        }

        # Publish
        producer_wrapper.publish_event(test_event)

        # Check if internal produce was called correctly
        # Extract arguments passed to internal producer
        produce_calls = mock_producer_instance.produce.call_args_list
        self.assertEqual(len(produce_calls), 1)

        args, kwargs = produce_calls[0]
        self.assertEqual(args[0], 'test_topic')
        
        # Key should be encoded string of the primary keys dict
        expected_key = str({"id": 1}).encode('utf-8')
        self.assertEqual(kwargs['key'], expected_key)
        
        # Value should be encoded JSON string of the event
        expected_value = json.dumps(test_event).encode('utf-8')
        self.assertEqual(kwargs['value'], expected_value)

        # Poll should be called
        mock_producer_instance.poll.assert_called_with(0)

if __name__ == '__main__':
    unittest.main()
