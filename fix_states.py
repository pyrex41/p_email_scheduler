import sqlite3
import json
import os
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_zip_data() -> Dict[str, Any]:
    """Load ZIP code data from zipData.json"""
    try:
        with open('zipData.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading zipData.json: {e}")
        raise

def get_state_from_zip(zip_code: str, zip_data: Dict[str, Any]) -> str:
    """Get state from ZIP code"""
    if not zip_code:
        return None
        
    try:
        # Clean and format the ZIP code
        zip_str = str(zip_code).strip()
        
        # Handle ZIP+4 format
        if '-' in zip_str:
            zip_str = zip_str.split('-')[0]
            
        # Remove any non-numeric characters
        zip_str = ''.join(c for c in zip_str if c.isdigit())
        
        # Ensure 5 digits with leading zeros
        zip_str = zip_str[:5].zfill(5)
        
        # Look up state
        return zip_data.get(zip_str, {}).get('state')
    except Exception as e:
        logger.error(f"Error looking up state for ZIP code {zip_code}: {e}")
        return None

def update_states_in_db(db_path: str, zip_data: Dict[str, Any]) -> None:
    """Update state codes in database based on ZIP codes"""
    logger.info(f"Processing database: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get total count of contacts
        cursor.execute("SELECT COUNT(*) FROM contacts")
        total_contacts = cursor.fetchone()[0]
        logger.info(f"Total contacts in database: {total_contacts}")
        
        # Get contacts with ZIP codes
        cursor.execute("""
            SELECT id, zip_code 
            FROM contacts 
            WHERE zip_code IS NOT NULL AND zip_code != ''
        """)
        
        # Process in batches
        batch_size = 1000
        updates = []
        total_updated = 0
        total_processed = 0
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
                
            for row in rows:
                contact_id, zip_code = row
                state = get_state_from_zip(zip_code, zip_data)
                
                if state:
                    updates.append((state, contact_id))
                    total_updated += 1
                
                total_processed += 1
                
                # Execute batch update when we reach batch size
                if len(updates) >= batch_size:
                    cursor.executemany(
                        "UPDATE contacts SET state = ? WHERE id = ?",
                        updates
                    )
                    conn.commit()
                    logger.info(f"Updated {len(updates)} contacts with state codes")
                    updates = []
                    
            # Progress update
            logger.info(f"Processed {total_processed}/{total_contacts} contacts ({(total_processed/total_contacts)*100:.1f}%)")
        
        # Final batch update
        if updates:
            cursor.executemany(
                "UPDATE contacts SET state = ? WHERE id = ?",
                updates
            )
            conn.commit()
            logger.info(f"Updated final batch of {len(updates)} contacts with state codes")
        
        # Verify results
        cursor.execute("""
            SELECT state, COUNT(*) as count 
            FROM contacts 
            WHERE state IS NOT NULL AND state != '' 
            GROUP BY state 
            ORDER BY count DESC
        """)
        
        logger.info("\nState distribution after update:")
        for row in cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]} contacts")
            
        logger.info(f"\nTotal contacts updated: {total_updated}")
        logger.info(f"Total contacts processed: {total_processed}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        conn.close()

def main():
    """Main function to update all organization databases"""
    try:
        # Load ZIP data
        logger.info("Loading ZIP code data...")
        zip_data = load_zip_data()
        logger.info("ZIP code data loaded successfully")
        
        # Get list of organization databases
        org_db_dir = "org_dbs"
        if not os.path.exists(org_db_dir):
            raise FileNotFoundError(f"Organization database directory not found: {org_db_dir}")
            
        org_dbs = [f for f in os.listdir(org_db_dir) if f.startswith('org-') and f.endswith('.db')]
        logger.info(f"Found {len(org_dbs)} organization databases")
        
        # Process each database
        for org_db in org_dbs:
            try:
                org_db_path = os.path.join(org_db_dir, org_db)
                org_id = int(org_db.replace('org-', '').replace('.db', ''))
                logger.info(f"\nProcessing organization {org_id} ({org_db})")
                update_states_in_db(org_db_path, zip_data)
            except Exception as e:
                logger.error(f"Error processing {org_db}: {e}")
                continue
                
        logger.info("\nCompleted state updates for all organization databases")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main() 