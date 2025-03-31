#!/usr/bin/env python3
import sqlite3
import sys
import os
import logging
from datetime import date
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the parse_date_flexible function from org_utils
from org_utils import parse_date_flexible

def normalize_dates_in_db(db_path: str) -> None:
    """
    Normalize dates in the SQLite database to ISO format (YYYY-MM-DD)
    
    Args:
        db_path: Path to the SQLite database file
    """
    if not os.path.exists(db_path):
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
        
    logger.info(f"Normalizing dates in database: {db_path}")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if contacts table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='contacts'")
        if not cursor.fetchone():
            logger.info(f"No contacts table found in {db_path}. Skipping.")
            return
            
        # Check which date columns exist and their constraints
        cursor.execute("PRAGMA table_info(contacts)")
        columns_info = cursor.fetchall()
        columns = {}
        
        for col in columns_info:
            col_name = col[1]
            not_null = col[3] == 1  # Check NOT NULL constraint
            if col_name in ['birth_date', 'effective_date']:
                columns[col_name] = {'not_null': not_null}
        
        date_columns = list(columns.keys())
        if not date_columns:
            logger.info(f"No date columns found in contacts table in {db_path}. Skipping.")
            return
        
        # Check if we need to modify the constraints temporarily
        constraints_modified = False
        if any(col['not_null'] for col in columns.values()):
            try:
                # Create a temporary table without NOT NULL constraints
                logger.info("Creating temporary table without NOT NULL constraints")
                cursor.execute("BEGIN TRANSACTION")
                
                # Get all column names from the contacts table
                cursor.execute("PRAGMA table_info(contacts)")
                all_cols = cursor.fetchall()
                column_defs = []
                for col in all_cols:
                    name = col[1]
                    type_name = col[2]
                    # Remove NOT NULL constraint for date columns
                    is_date_col = name in date_columns
                    not_null = col[3] == 1 and not is_date_col
                    pk = col[5] == 1
                    
                    if pk:
                        column_defs.append(f"{name} {type_name} PRIMARY KEY")
                    elif not_null:
                        column_defs.append(f"{name} {type_name} NOT NULL")
                    else:
                        column_defs.append(f"{name} {type_name}")
                
                # Create temporary table
                cursor.execute(f"CREATE TABLE contacts_temp ({', '.join(column_defs)})")
                
                # Copy data
                cursor.execute(f"INSERT INTO contacts_temp SELECT * FROM contacts")
                
                # Drop old table and rename temp
                cursor.execute("DROP TABLE contacts")
                cursor.execute("ALTER TABLE contacts_temp RENAME TO contacts")
                
                conn.commit()
                constraints_modified = True
                logger.info("Successfully removed NOT NULL constraints from date columns")
            except sqlite3.Error as e:
                logger.error(f"Error modifying table constraints: {e}")
                conn.rollback()
                # Continue with original table
            
        # Process each date column
        for column in date_columns:
            logger.info(f"Normalizing {column} in {db_path}")
            
            # Get all rows with non-null and non-empty values in the column
            cursor.execute(f"SELECT id, {column} FROM contacts WHERE {column} IS NOT NULL AND {column} != ''")
            rows = cursor.fetchall()
            
            if not rows:
                logger.info(f"No {column} values to normalize in {db_path}")
                continue
                
            # Sample data for logging
            sample_data = rows[:5]
            logger.info(f"Sample {column} values before normalization: {[row[1] for row in sample_data]}")
            
            # Initialize counters
            total = len(rows)
            updated = 0
            skipped = 0
            
            # Process each row
            for row in rows:
                row_id, date_value = row
                
                # Skip if already in ISO format (YYYY-MM-DD)
                if (isinstance(date_value, str) and 
                    len(date_value) == 10 and 
                    date_value[4] == '-' and 
                    date_value[7] == '-'):
                    continue
                    
                # Parse and normalize the date
                parsed_date = parse_date_flexible(date_value)
                
                if parsed_date:
                    # Update the row with ISO formatted date
                    cursor.execute(
                        f"UPDATE contacts SET {column} = ? WHERE id = ?", 
                        (parsed_date.isoformat(), row_id)
                    )
                    updated += 1
                else:
                    # Keep the original value if parsing fails and the column has NOT NULL constraint
                    if columns[column].get('not_null', False):
                        logger.debug(f"Couldn't parse {column} value '{date_value}' for row {row_id}, keeping original value")
                    else:
                        # Only set to NULL if column allows NULL
                        cursor.execute(
                            f"UPDATE contacts SET {column} = NULL WHERE id = ?", 
                            (row_id,)
                        )
                    skipped += 1
                    
            # Commit changes for this column
            conn.commit()
            
            # Get sample of normalized data
            cursor.execute(f"SELECT {column} FROM contacts WHERE {column} IS NOT NULL LIMIT 5")
            normalized_samples = cursor.fetchall()
            logger.info(f"Sample {column} values after normalization: {[row[0] for row in normalized_samples]}")
            
            # Log summary
            logger.info(f"Normalized {column} in {db_path}: {updated} updated, {skipped} skipped, {total} total")
            
        # Create index on date columns for better query performance
        for column in date_columns:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{column} ON contacts({column})")
            except sqlite3.Error as e:
                logger.warning(f"Could not create index on {column}: {e}")
                
        conn.commit()
        logger.info(f"Successfully normalized dates in {db_path}")
        
    except sqlite3.Error as e:
        logger.error(f"Error normalizing dates in {db_path}: {e}")
        if 'conn' in locals():
            conn.rollback()
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python normalize_dates.py <database_path>")
        sys.exit(1)
        
    db_path = sys.argv[1]
    normalize_dates_in_db(db_path) 