import logging
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import mysql.connector # type: ignore
from mysql.connector.errors import Error # type: ignore

logger = logging.getLogger(__name__)

class CDCProcessor:
    def __init__(self, host, port, user, password, database, table_name):
        self.db_config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
        self.table_name = table_name
        
        self.last_check_time = None
        self.known_records: Dict[Any, Dict[str, Any]] = {}  # Cache of known records for detecting DELETEs and UPDATE old_data
        
        self.conn: Any = None
        self.connect()
        # Initialize internal state so we don't treat everything as new insertions immediately 
        # unless it actually is the first run.
        self.initialize_state()

    def connect(self):
        """Establishes connection to the MySQL database."""
        try:
            if self.conn is None or not self.conn.is_connected():
                self.conn = mysql.connector.connect(**self.db_config)
                logger.info(f"Connected to MySQL DB: {self.db_config['database']}")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            self.conn = None

    def initialize_state(self):
        """Fetches the current state of the table to initialize known records."""
        if not self.conn:
            return
            
        logger.info(f"Initializing CDC state for table {self.table_name}...")
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {self.table_name}")
            rows = cursor.fetchall()
            
            for row in rows:
                if 'id' in row:
                    self.known_records[row['id']] = row
                    
            cursor.execute("SELECT NOW()")
            self.last_check_time = cursor.fetchone()['NOW()']
            logger.info(f"Initialized state with {len(self.known_records)} records at {self.last_check_time}")
            cursor.close()
        except Error as e:
            logger.error(f"Failed to initialize state: {e}")

    def _create_event(self, operation: str, pk: Dict[str, Any], old_data: Optional[Dict], new_data: Optional[Dict]) -> Dict[str, Any]:
        """Helper to format a raw database change into the standardized CDC event schema."""
        # Convert datetime objects to string for JSON serialization
        if old_data and 'last_updated' in old_data and isinstance(old_data['last_updated'], datetime):
            old_data['last_updated'] = old_data['last_updated'].isoformat()
        if new_data and 'last_updated' in new_data and isinstance(new_data['last_updated'], datetime):
            new_data['last_updated'] = new_data['last_updated'].isoformat()
            
        # Decimal serialization fix
        from decimal import Decimal
        def serialize_decimals(payload):
            if not payload:
                return payload
            for k, v in payload.items():
                if isinstance(v, Decimal):
                    payload[k] = float(v)
            return payload
            
        old_data = serialize_decimals(old_data)
        new_data = serialize_decimals(new_data)

        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "table_name": self.table_name,
            "operation_type": operation,
            "primary_keys": pk,
            "payload": {
                "old_data": old_data,
                "new_data": new_data
            }
        }

    def poll_changes(self) -> List[Dict[str, Any]]:
        """Polls the database for changes and returns a list of CDC events."""
        events = []
        if not self.conn or not self.conn.is_connected():
            self.connect()
            if not self.conn:
                return events

        try:
            cursor = self.conn.cursor(dictionary=True)
            
            # 1. Grab current time to update last_check_time after successful polling
            cursor.execute("SELECT NOW()")
            current_check_time = cursor.fetchone()['NOW()']

            # 2. Check for INSERTs and UPDATEs
            query = f"SELECT * FROM {self.table_name} WHERE last_updated > %s"
            cursor.execute(query, (self.last_check_time,))
            changed_rows = cursor.fetchall()

            current_ids = set()
            
            # Process inserts and updates
            for row in changed_rows:
                row_id = row['id']
                current_ids.add(row_id)
                pk = {"id": row_id}
                
                if row_id in self.known_records:
                    # Update
                    old_row = self.known_records[row_id]
                    events.append(self._create_event("UPDATE", pk, old_data=old_row, new_data=dict(row)))
                else:
                    # Insert
                    events.append(self._create_event("INSERT", pk, old_data=None, new_data=dict(row)))
                
                # Update memory cache
                self.known_records[row_id] = row
                
            # 3. Check for DELETEs
            # We fetch all IDs currently in the table to compare against our cache
            cursor.execute(f"SELECT id FROM {self.table_name}")
            all_current_ids_result = cursor.fetchall()
            all_current_ids = {r['id'] for r in all_current_ids_result}
            
            deleted_ids = set(self.known_records.keys()) - all_current_ids
            
            for row_id in deleted_ids:
                old_row = self.known_records[row_id]
                pk = {"id": row_id}
                events.append(self._create_event("DELETE", pk, old_data=old_row, new_data=None))
                # Remove from cache
                self.known_records.pop(row_id, None)

            self.last_check_time = current_check_time
            cursor.close()
            
        except Error as e:
            logger.error(f"Error during CDC polling: {e}")
            # Ensure connection is re-established next time if it broke
            try:
                self.conn.ping(reconnect=True, attempts=1, delay=0)
            except Error:
                self.conn = None
                
        return events

    def close(self):
        """Closes the MySQL connection."""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logger.info("MySQL DB connection closed.")
