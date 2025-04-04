import json
import sqlite3
import os
import sys
from typing import List, Dict, Any, Optional
import logging
from dotenv import load_dotenv
from datetime import date, datetime
import pandas as pd

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# Load ZIP data once at module level
try:
    with open('zipData.json', 'r') as f:
        ZIP_DATA = json.load(f)
except Exception as e:
    logger.error(f"Error loading zipData.json: {e}")
    ZIP_DATA = {}

def connect_to_db(db_path: str) -> sqlite3.Connection:
    """
    Connect to SQLite database and set row factory for dictionary results
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        SQLite connection object
    """
    if not os.path.exists(db_path):
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database {db_path}: {e}")
        sys.exit(1)

def get_organization_details(main_db_path: str, org_id: int) -> Dict[str, Any]:
    """
    Get organization details from the main database
    
    Args:
        main_db_path: Path to the main database
        org_id: Organization ID
        
    Returns:
        Organization details as a dictionary
    """
    logger.info(f"Getting organization details for org_id: {org_id}")
    
    conn = connect_to_db(main_db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, turso_db_url, turso_auth_token FROM organizations WHERE id = ?", (org_id,))
        org = cursor.fetchone()
        
        if not org:
            logger.error(f"Organization with ID {org_id} not found in the database")
            sys.exit(1)
            
        return dict(org)
    except sqlite3.Error as e:
        logger.error(f"Error retrieving organization details: {e}")
        sys.exit(1)
    finally:
        conn.close()

def get_state_from_zip(zip_code: str) -> str:
    """Get the state from a ZIP code using zipData.json"""
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
        state = ZIP_DATA.get(zip_str, {}).get('state')
        if state:
            logger.debug(f"Found state {state} for ZIP code {zip_str}")
            return state
        else:
            logger.warning(f"No state found for ZIP code {zip_str}")
            return None
    except Exception as e:
        logger.error(f"Error looking up state for ZIP code {zip_code}: {e}")
        return None
    
def get_n_contacts_from_org_db(org_db_path: str, org_id: int, n: int) -> List[Dict[str, Any]]:
    """
    Get n contacts from an organization's database
    """
    with sqlite3.connect(org_db_path) as conn:
        """
        Get n random contact IDs from the organization's database
        
        Args:
            org_db_path: Path to the organization's SQLite database
            org_id: Organization ID
            n: Number of random contacts to retrieve
            
        Returns:
            List of n random contact dictionaries
        """
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get n random contact IDs
        cursor.execute("""
            SELECT id FROM contacts
            ORDER BY RANDOM()
            LIMIT ?
        """, (n,))
        
        rows = cursor.fetchall()
        n_contact_ids = [row['id'] for row in rows]
        
        if not n_contact_ids:
            logger.warning(f"No contacts found in database for org_id: {org_id}")
            return []
    
    return get_contacts_from_org_db(org_db_path, org_id, contact_ids=n_contact_ids)

def get_contacts_from_org_db(org_db_path: str, org_id: int, contact_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Get contacts from an organization's database
    
    Args:
        org_db_path: Path to the organization's SQLite database
        org_id: Organization ID
        contact_ids: Optional list of contact IDs to filter by
        
    Returns:
        List of contact dictionaries
    """
    contacts = []
    
    # Build the SQL query
    sql = """
        SELECT id, first_name, last_name, email, state, birth_date, effective_date
        FROM contacts
        WHERE 1=1
    """
    params = []
    
    # Add contact ID filter if provided
    if contact_ids:
        # Convert all IDs to strings for comparison
        str_ids = [str(cid) for cid in contact_ids]
        sql += " AND CAST(id AS TEXT) IN ({})".format(','.join(['?' for _ in str_ids]))
        params.extend(str_ids)
    
    try:
        with sqlite3.connect(org_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            for row in rows:
                contact = dict(row)
                # Convert dates to ISO format strings if they exist
                if contact.get('birth_date'):
                    contact['birth_date'] = pd.to_datetime(contact['birth_date']).date().isoformat()
                if contact.get('effective_date'):
                    contact['effective_date'] = pd.to_datetime(contact['effective_date']).date().isoformat()
                contacts.append(contact)
                
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []
        
    return contacts

def get_filtered_contacts_from_org_db(org_db_path: str, org_id: int, 
                                      effective_date_age_years: Optional[int] = None,
                                      effective_date_start: Optional[str] = None,
                                      effective_date_end: Optional[str] = None,
                                      states: Optional[List[str]] = None, n: Optional[int] = None, is_random: bool = False) -> List[Dict[str, Any]]:
    """
    Get contacts from the organization's database with filtering by effective date range and states
    
    Args:
        org_db_path: Path to the organization's database
        org_id: Organization ID
        effective_date_age_years: Filter contacts by effective date age relative to current year (legacy parameter)
                                 Positive values (e.g., 2) mean "2+ years old"
                                 Negative values (e.g., -2) mean "within next 2 years"
                                 Zero means "this year" (current year)
        effective_date_start: Start of effective date range in format "YYYY-MM" (e.g., "2018-01")
        effective_date_end: End of effective date range in format "YYYY-MM" (e.g., "2020-12")
                          When using months-ago format:
                          - effective_date_start is the OLDER date (e.g., 36 months ago)
                          - effective_date_end is the NEWER date (e.g., 24 months ago or -1 for unlimited)
        states: Filter contacts to include only those in the specified states
        n: Optional limit on number of results to return
        is_random: If True and n is provided, randomly sample n results
        
    Returns:
        List of contacts as dictionaries
    """
    logger.info(f"Getting filtered contacts from organization database: {org_db_path}")
    logger.debug(f"Filter params - effective_date_start: {effective_date_start}, effective_date_end: {effective_date_end}, states: {states}, n: {n}, is_random: {is_random}")
    
    conn = connect_to_db(org_db_path)
    try:
        cursor = conn.cursor()
        
        # First, let's get a total count of contacts to understand our baseline
        cursor.execute("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email != ''")
        total_contacts = cursor.fetchone()[0]
        logger.info(f"Total contacts in database before filtering: {total_contacts}")
        
        # Check if the contacts table exists and has the required columns
        cursor.execute("PRAGMA table_info(contacts)")
        columns = {column['name']: True for column in cursor.fetchall()}
        logger.debug(f"Available columns in contacts table: {list(columns.keys())}")
        
        # Verify critical columns exist
        if 'email' not in columns:
            raise ValueError("Missing critical column 'email' in contacts table")
            
        # Build query with filtering for valid data
        query_parts = []
        
        # Handle ID column specially
        if 'id' in columns:
            query_parts.append('id')
        else:
            query_parts.append('rowid as id')
            
        # Add email (required)
        query_parts.append('email')
        
        # Add all required columns if they exist
        required_columns = ['first_name', 'last_name', 'birth_date', 'effective_date', 'zip_code', 'gender']
        for col in required_columns:
            if col in columns:
                query_parts.append(col)
        
        # Basic WHERE clause - only check for critical data
        where_conditions = ['email IS NOT NULL AND email != ""']
        params = []

        # Add effective date filtering
        if effective_date_start is not None or effective_date_end is not None:
            # Add effective date column to query if not already included
            if 'effective_date' not in query_parts:
                query_parts.append('effective_date')
            
            # Add effective date range conditions
            if effective_date_start is not None:
                # For months-ago format, effective_date_start is the OLDER date
                # So we want effective_date <= start_date (older than or equal to start_date)
                where_conditions.append('date(effective_date) <= date(?)')
                params.append(f"{effective_date_start}-01")  # Add day for proper date comparison
                logger.debug(f"Added start date filter: <= {effective_date_start}-01")
            if effective_date_end is not None and effective_date_end != "-1":
                # For months-ago format, effective_date_end is the NEWER date
                # So we want effective_date >= end_date (newer than or equal to end_date)
                where_conditions.append('date(effective_date) >= date(?)')
                params.append(f"{effective_date_end}-01")  # Add day for proper date comparison
                logger.debug(f"Added end date filter: >= {effective_date_end}-01")
        elif effective_date_age_years is not None:
            # Add effective date column to query if not already included
            if 'effective_date' not in query_parts:
                query_parts.append('effective_date')
            
            # Calculate date range based on current year
            current_year = date.today().year
            if effective_date_age_years > 0:
                # Filter for contacts with effective date older than X years
                target_year = current_year - effective_date_years
                where_conditions.append('strftime("%Y", effective_date) <= ?')
                params.append(str(target_year))
            elif effective_date_age_years < 0:
                # Filter for contacts with effective date within next X years
                target_year = current_year - effective_date_years
                where_conditions.append('strftime("%Y", effective_date) >= ?')
                params.append(str(target_year))
            else:
                # Filter for contacts with effective date in current year
                where_conditions.append('strftime("%Y", effective_date) = ?')
                params.append(str(current_year))

        # If states are provided, filter by ZIP codes that map to those states
        if states and len(states) > 0:
            # Get all ZIP codes that map to the requested states
            state_zip_codes = []
            for zip_code, data in ZIP_DATA.items():
                if data.get('state') in states:
                    state_zip_codes.append(zip_code)
            
            if state_zip_codes:
                zip_placeholders = ','.join(['?' for _ in state_zip_codes])
                where_conditions.append(f'zip_code IN ({zip_placeholders})')
                params.extend(state_zip_codes)
            else:
                logger.warning(f"No ZIP codes found for states: {states}")
                return []
        
        # Build the query
        query = f"""
            SELECT {', '.join(query_parts)} 
            FROM contacts 
            WHERE {' AND '.join(where_conditions)}
        """
        
        # Add ORDER BY RANDOM() if is_random is True and n is provided
        if n is not None and is_random:
            query += " ORDER BY RANDOM()"
            
        # Add LIMIT if n is provided
        if n is not None:
            query += f" LIMIT {n}"
        
        logger.debug(f"Executing SQL query: {query}")
        logger.debug(f"Query parameters: {params}")
        
        # Execute query
        cursor.execute(query, params)
        
        contacts = []
        # Process rows in batches of 1000
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
                
            for row in rows:
                contact = dict(row)
                contact['organization_id'] = org_id
                
                # Always try to determine state from ZIP code
                if contact.get('zip_code'):
                    state = get_state_from_zip(contact['zip_code'])
                    if state:
                        contact['state'] = state
                        contacts.append(contact)
                    else:
                        logger.debug(f"Could not determine state from ZIP code {contact.get('zip_code')} for contact {contact.get('id')}")
                else:
                    logger.debug(f"No ZIP code found for contact {contact.get('id')}")
            
        logger.info(f"Retrieved {len(contacts)} contacts from organization database with filters")
        return contacts
    except sqlite3.Error as e:
        logger.error(f"Error retrieving filtered contacts: {e}")
        raise
    finally:
        conn.close()

def parse_date_flexible(date_str: str) -> Optional[date]:
    """Parse a date string flexibly, handling various formats and cleaning input"""
    if not date_str:
        return None
        
    MIN_YEAR = 1900
    MAX_YEAR = 2100
    
    # Month name mappings
    MONTH_MAP = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
        
    # Clean the input string
    orig_date_str = str(date_str).strip()
    
    def validate_year(year: int) -> bool:
        """Validate year is within acceptable range"""
        return MIN_YEAR <= year <= MAX_YEAR
    
    def validate_month(month: int) -> bool:
        """Validate month is between 1-12"""
        return 1 <= month <= 12
        
    def validate_day(year: int, month: int, day: int) -> bool:
        """Validate day is valid for given month/year"""
        try:
            date(year, month, day)
            return True
        except ValueError:
            return False
            
    def normalize_year(year: int) -> int:
        """Convert 2-digit year to 4-digit year (always assume 19XX)"""
        if year < 100:
            return 1900 + year
        return year

    # First try to handle month abbreviation formats (e.g., "Jul-59", "26-Jul-57")
    parts = orig_date_str.replace('/', '-').split('-')
    if len(parts) in [2, 3]:
        try:
            # Handle "MMM-YY" format (e.g., "Jul-59")
            if len(parts) == 2:
                month_str = parts[0].lower()
                if month_str in MONTH_MAP:
                    month = MONTH_MAP[month_str]
                    year = normalize_year(int(parts[1]))
                    if validate_year(year) and validate_month(month):
                        return date(year, month, 1)
            
            # Handle "DD-MMM-YY" format (e.g., "26-Jul-57")
            elif len(parts) == 3:
                day = int(parts[0])
                month_str = parts[1].lower()
                if month_str in MONTH_MAP:
                    month = MONTH_MAP[month_str]
                    year = normalize_year(int(parts[2]))
                    if validate_year(year) and validate_month(month) and validate_day(year, month, day):
                        return date(year, month, day)
        except (ValueError, IndexError):
            pass

    # Clean up date string for further processing
    date_str = orig_date_str.replace('`', '').replace('//', '/').replace('  ', ' ').replace(' ', '')

    # Handle year-only format (e.g., "1923")
    if date_str.isdigit() and len(date_str) == 4:
        year = int(date_str)
        if validate_year(year):
            return date(year, 1, 1)
    
    # Handle compressed dates without any separators
    if date_str.isdigit():
        # Try as MMDDYYYY or MMDDYY
        if len(date_str) in [8, 6]:
            try:
                month = int(date_str[:2])
                day = int(date_str[2:4])
                year = int(date_str[4:])
                year = normalize_year(year)
                if validate_year(year) and validate_month(month) and validate_day(year, month, day):
                    return date(year, month, day)
            except ValueError:
                pass
                
        # Try as DDMMYYYY or DDMMYY
        if len(date_str) in [8, 6]:
            try:
                day = int(date_str[:2])
                month = int(date_str[2:4])
                year = int(date_str[4:])
                year = normalize_year(year)
                if validate_year(year) and validate_month(month) and validate_day(year, month, day):
                    return date(year, month, day)
            except ValueError:
                pass

    # Clean up date string for parsing
    date_str = date_str.replace('/', '').replace('-', '').replace('.', '').replace(' ', '')
    
    # Try standard formats with cleaned string
    formats = [
        "%Y%m%d",     # YYYYMMDD
        "%d%m%Y",     # DDMMYYYY
        "%m%d%Y",     # MMDDYYYY
        "%Y%m",       # YYYYMM (will default to day 1)
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()
            if validate_year(parsed_date.year):
                return parsed_date
        except ValueError:
            continue

    # If we still haven't found a match, try one last time with very flexible parsing
    try:
        # Extract all numbers from the string
        nums = ''.join(c for c in date_str if c.isdigit())
        if len(nums) >= 6:
            # Try to interpret as month/day/year
            if len(nums) >= 8:
                month = int(nums[:2])
                day = int(nums[2:4])
                year = int(nums[4:8])
            else:
                month = int(nums[:2])
                day = int(nums[2:4])
                year = int(nums[4:])
            year = normalize_year(year)
            if validate_year(year) and validate_month(month) and validate_day(year, month, day):
                return date(year, month, day)
    except (ValueError, IndexError):
        pass

    return None

def format_contact_data(contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Format contact data for compatibility with the email scheduler"""
    logger.info("Formatting contact data for scheduler")
    
    formatted_contacts = []
    for contact in contacts:
        # Always try to determine state from ZIP code first
        state = None
        if contact.get('zip_code'):
            state = get_state_from_zip(contact['zip_code'])
        
        # If we couldn't get state from ZIP, check if existing state is valid
        if not state:
            state = contact.get('state')
        
        # Skip contacts without a valid state
        if not state:
            logger.warning(f"Skipping contact {contact.get('id')}: Missing valid state")
            continue
        
        # Ensure required fields exist
        formatted_contact = {
            'id': contact.get('id'),
            'contact_id': str(contact.get('id')),
            'first_name': contact.get('first_name', 'Unknown'),
            'last_name': contact.get('last_name', 'Unknown'),
            'email': contact.get('email', f"contact{contact.get('id')}@example.com"),
            'birth_date': contact.get('birth_date'),
            'effective_date': contact.get('effective_date'),
            'state': state,
            'organization_id': contact.get('organization_id')
        }
        
        # Skip contacts with missing critical data
        if not formatted_contact['birth_date'] and not formatted_contact['effective_date']:
            logger.warning(f"Skipping contact {formatted_contact['id']}: Missing both birth_date and effective_date")
            continue
            
        # Convert date fields if needed
        for date_field in ['birth_date', 'effective_date']:
            if formatted_contact[date_field]:
                logger.debug(f"Processing {date_field}: {formatted_contact[date_field]}")
                if not isinstance(formatted_contact[date_field], date):
                    if isinstance(formatted_contact[date_field], str):
                        parsed_date = parse_date_flexible(formatted_contact[date_field])
                        if parsed_date:
                            formatted_contact[date_field] = parsed_date.isoformat()
                            logger.debug(f"Parsed {date_field} to {formatted_contact[date_field]}")
                        else:
                            logger.warning(f"Could not parse {date_field} for contact {formatted_contact['id']}: {formatted_contact[date_field]}")
                            formatted_contact[date_field] = None
                    else:
                        formatted_contact[date_field] = formatted_contact[date_field].isoformat()
                        logger.debug(f"Converted {date_field} to ISO format: {formatted_contact[date_field]}")
                
        logger.debug(f"Final formatted contact: {formatted_contact}")
        formatted_contacts.append(formatted_contact)
        
    logger.info(f"Formatted {len(formatted_contacts)} contacts for scheduling")
    return formatted_contacts

def update_states_from_zip_codes(org_db_path: str) -> None:
    """
    Update state information in the database using ZIP codes.
    This function will:
    1. Find all contacts with missing/empty states but valid ZIP codes
    2. Update their state based on the ZIP data
    
    Args:
        org_db_path: Path to the organization's database
    """
    logger.info(f"Updating state information from ZIP codes in database: {org_db_path}")
    
    conn = connect_to_db(org_db_path)
    try:
        cursor = conn.cursor()
        
        # First, get count of contacts with missing states but valid ZIP codes
        cursor.execute("""
            SELECT COUNT(*) 
            FROM contacts 
            WHERE (state IS NULL OR state = '') 
            AND zip_code IS NOT NULL 
            AND zip_code != ''
        """)
        missing_states_count = cursor.fetchone()[0]
        logger.info(f"Found {missing_states_count} contacts with missing states but valid ZIP codes")
        
        if missing_states_count == 0:
            logger.info("No contacts need state updates")
            return
            
        # Get all contacts that need updating
        cursor.execute("""
            SELECT id, zip_code 
            FROM contacts 
            WHERE (state IS NULL OR state = '') 
            AND zip_code IS NOT NULL 
            AND zip_code != ''
        """)
        
        # Process in batches to avoid memory issues
        batch_size = 1000
        updates = []
        total_updated = 0
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
                
            for row in rows:
                contact_id = row['id']
                zip_code = str(row['zip_code'])[:5].zfill(5)  # Ensure 5-digit format
                state = get_state_from_zip(zip_code)
                
                if state:
                    updates.append((state, contact_id))
                    
            # Execute batch update
            if updates:
                cursor.executemany(
                    "UPDATE contacts SET state = ? WHERE id = ?",
                    updates
                )
                total_updated += len(updates)
                updates = []  # Clear for next batch
                
        conn.commit()
        logger.info(f"Updated state information for {total_updated} contacts")
        
        # Verify results
        cursor.execute("""
            SELECT state, COUNT(*) as count 
            FROM contacts 
            WHERE state IS NOT NULL AND state != '' 
            GROUP BY state
        """)
        state_counts = cursor.fetchall()
        logger.info("State distribution after update:")
        for row in state_counts:
            logger.info(f"  {row['state']}: {row['count']} contacts")
            
    except sqlite3.Error as e:
        logger.error(f"Database error while updating states: {e}")
        raise
    finally:
        conn.close()

def update_all_org_dbs_states() -> None:
    """
    Update state information in all organization databases.
    This function will scan the org_dbs directory and update each database.
    """
    logger.info("Starting state update for all organization databases")
    
    # Get list of all org databases
    org_db_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "org_dbs")
    if not os.path.exists(org_db_dir):
        logger.warning(f"Organization database directory not found: {org_db_dir}")
        return
        
    org_dbs = [f for f in os.listdir(org_db_dir) if f.startswith('org-') and f.endswith('.db')]
    logger.info(f"Found {len(org_dbs)} organization databases")
    
    for org_db in org_dbs:
        try:
            org_db_path = os.path.join(org_db_dir, org_db)
            org_id = int(org_db.replace('org-', '').replace('.db', ''))
            logger.info(f"Processing organization {org_id} ({org_db})")
            update_states_from_zip_codes(org_db_path)
        except Exception as e:
            logger.error(f"Error processing {org_db}: {e}")
            continue
            
    logger.info("Completed state updates for all organization databases")