import unittest
from datetime import datetime
from src.cdc_processor import CDCProcessor # type: ignore

class TestCDCProcessor(unittest.TestCase):
    def setUp(self):
        # We don't actually want to connect to a DB for unit tests of the transformation logic
        # So we mock out the connect method before instantiation
        original_connect = CDCProcessor.connect
        original_init_state = CDCProcessor.initialize_state
        
        CDCProcessor.connect = lambda self: None
        CDCProcessor.initialize_state = lambda self: None
        
        self.processor = CDCProcessor(
            host="localhost",
            port=3306,
            user="user",
            password="password",
            database="test_db",
            table_name="test_table"
        )
        
        # Restore mock
        CDCProcessor.connect = original_connect
        CDCProcessor.initialize_state = original_init_state

    def test_create_event_insert(self):
        pk = {"id": 1}
        new_data = {"id": 1, "name": "Test Item", "price": 10.5, "stock": 100}
        
        event = self.processor._create_event("INSERT", pk, None, new_data)
        
        self.assertEqual(event["operation_type"], "INSERT")
        self.assertEqual(event["table_name"], "test_table")
        self.assertEqual(event["primary_keys"], {"id": 1})
        self.assertIsNone(event["payload"]["old_data"])
        self.assertEqual(event["payload"]["new_data"]["name"], "Test Item")
        
        # Test UUID and Timestamp presence
        self.assertTrue("event_id" in event)
        self.assertTrue("timestamp" in event)

    def test_create_event_update(self):
        pk = {"id": 1}
        old_data = {"id": 1, "name": "Test Item", "price": 10.5, "stock": 100}
        new_data = {"id": 1, "name": "Test Item", "price": 15.0, "stock": 90}
        
        event = self.processor._create_event("UPDATE", pk, old_data, new_data)
        
        self.assertEqual(event["operation_type"], "UPDATE")
        self.assertEqual(event["payload"]["old_data"]["price"], 10.5)
        self.assertEqual(event["payload"]["new_data"]["price"], 15.0)

    def test_create_event_delete(self):
        pk = {"id": 1}
        old_data = {"id": 1, "name": "Test Item", "price": 10.5, "stock": 100}
        
        event = self.processor._create_event("DELETE", pk, old_data, None)
        
        self.assertEqual(event["operation_type"], "DELETE")
        self.assertIsNone(event["payload"]["new_data"])
        self.assertEqual(event["payload"]["old_data"]["name"], "Test Item")

if __name__ == '__main__':
    unittest.main()
