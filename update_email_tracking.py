"""
Update email tracking schema for all organization databases.
Applies the updated email_send_tracking table schema to all databases.
"""

import os
import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger("schema_update")

def get_org_dbs():
    """Get all organization database paths."""
    org_db_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "org_dbs")
    if not os.path.exists(org_db_dir):
        logger.warning(f"Organization database directory not found: {org_db_dir}")
        return []
    
    # Find all organization databases
    org_dbs = []
    for file in os.listdir(org_db_dir):
        if file.startswith('org-') and file.endswith('.db'):
            org_dbs.append(os.path.join(org_db_dir, file))
    
    logger.info(f"Found {len(org_dbs)} organization databases to update")
    return org_dbs

def apply_migration(db_path):
    """Apply migration to a database."""
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if email_send_tracking table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'")
        if not cursor.fetchone():
            logger.info(f"Table email_send_tracking does not exist in {db_path}, skipping")
            conn.close()
            return False
        
        # Check if columns already exist to avoid duplicate migrations
        cursor.execute("PRAGMA table_info(email_send_tracking)")
        columns = [col[1] for col in cursor.fetchall()]
        
        new_columns = []
        if 'message_id' not in columns:
            new_columns.append(("message_id", "TEXT"))
        if 'delivery_status' not in columns:
            new_columns.append(("delivery_status", "TEXT"))
        if 'status_checked_at' not in columns:
            new_columns.append(("status_checked_at", "TEXT"))
        if 'status_details' not in columns:
            new_columns.append(("status_details", "TEXT"))
        
        if not new_columns:
            logger.info(f"No new columns to add for {db_path}, skipping")
            conn.close()
            return False
        
        # Add new columns
        for column_name, column_type in new_columns:
            try:
                cursor.execute(f"ALTER TABLE email_send_tracking ADD COLUMN {column_name} {column_type}")
                logger.info(f"Added column {column_name} to {db_path}")
            except sqlite3.OperationalError as e:
                logger.warning(f"Error adding column {column_name} to {db_path}: {e}")
        
        # Update the CHECK constraint for send_status 
        # This is tricky in SQLite as you can't directly alter CHECK constraints
        # So we'll create a new table with the updated constraint and copy the data
        
        try:
            # First check for views that depend on this table and drop them temporarily
            cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND sql LIKE '%email_send_tracking%'")
            views = [row[0] for row in cursor.fetchall()]
            
            # Get the SQL for each view so we can recreate them later
            view_definitions = {}
            for view_name in views:
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='view' AND name='{view_name}'")
                view_sql = cursor.fetchone()[0]
                view_definitions[view_name] = view_sql
                logger.info(f"Found view {view_name} depending on email_send_tracking")
            
            # Start a transaction
            conn.execute("BEGIN TRANSACTION")
            
            # Drop dependent views
            for view_name in views:
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                    logger.info(f"Dropped view {view_name}")
                except sqlite3.OperationalError as e:
                    logger.warning(f"Error dropping view {view_name}: {e}")
            
            # Create new table with updated constraint
            conn.execute("""
                CREATE TABLE email_send_tracking_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    org_id INTEGER NOT NULL,
                    contact_id TEXT NOT NULL,
                    email_type TEXT NOT NULL,
                    scheduled_date TEXT NOT NULL,
                    send_status TEXT NOT NULL CHECK(send_status IN ('pending', 'processing', 'accepted', 'delivered', 'sent', 'deferred', 'bounced', 'dropped', 'failed', 'skipped')) DEFAULT 'pending',
                    send_mode TEXT NOT NULL CHECK(send_mode IN ('test', 'production')) DEFAULT 'test',
                    test_email TEXT,
                    send_attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_attempt_date TEXT,
                    last_error TEXT,
                    batch_id TEXT NOT NULL,
                    message_id TEXT,
                    delivery_status TEXT,
                    status_checked_at TEXT,
                    status_details TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Copy data from old table to new table
            conn.execute("""
                INSERT INTO email_send_tracking_new 
                SELECT id, org_id, contact_id, email_type, scheduled_date, 
                       send_status, send_mode, test_email, send_attempt_count, 
                       last_attempt_date, last_error, batch_id, 
                       message_id, delivery_status, status_checked_at, status_details,
                       created_at, updated_at
                FROM email_send_tracking
            """)
            
            # Drop old table
            conn.execute("DROP TABLE email_send_tracking")
            
            # Rename new table to original name
            conn.execute("ALTER TABLE email_send_tracking_new RENAME TO email_send_tracking")
            
            # Recreate indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_batch_id ON email_send_tracking(batch_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_send_status ON email_send_tracking(send_status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_send_mode ON email_send_tracking(send_mode)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_contact_id ON email_send_tracking(contact_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_contact_type ON email_send_tracking(contact_id, email_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_status_date ON email_send_tracking(send_status, scheduled_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_message_id ON email_send_tracking(message_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_tracking_delivery_status ON email_send_tracking(delivery_status)")
            
            # Recreate trigger
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS update_email_tracking_timestamp 
                AFTER UPDATE ON email_send_tracking 
                BEGIN
                    UPDATE email_send_tracking SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id; 
                END
            """)
            
            # Try to recreate views (if they don't reference missing tables)
            for view_name, view_sql in view_definitions.items():
                try:
                    # Check if the view references a non-existent table
                    if "contacts" in view_sql and "main.contacts" in view_sql:
                        # Skip the view if it references contacts table
                        logger.warning(f"Skipping view {view_name} because it references missing 'contacts' table")
                        continue
                    
                    # Execute the view creation SQL
                    conn.execute(view_sql)
                    logger.info(f"Recreated view {view_name}")
                except sqlite3.OperationalError as e:
                    logger.warning(f"Unable to recreate view {view_name}: {e}")
            
            # Commit transaction
            conn.commit()
            logger.info(f"Updated table schema with new status values in {db_path}")
            
        except Exception as e:
            # Rollback on error
            conn.rollback()
            logger.error(f"Error updating table schema in {db_path}: {e}")
            conn.close()
            return False
        
        # Close connection
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error migrating {db_path}: {e}")
        return False

def main():
    """Main entry point for migration script."""
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Get all organization databases
    org_dbs = get_org_dbs()
    
    # Apply migration to each database
    success_count = 0
    failed_count = 0
    
    for db_path in org_dbs:
        logger.info(f"Applying migration to {db_path}")
        if apply_migration(db_path):
            success_count += 1
        else:
            failed_count += 1
    
    # Log summary
    logger.info(f"Migration complete: {success_count} successful, {failed_count} failed")
    print(f"Migration complete: {success_count} successful, {failed_count} failed")

if __name__ == "__main__":
    main()