import json
import sqlite3
import os
import sys
from typing import List, Dict, Any, Optional
import logging
from dotenv import load_dotenv
from datetime import date, datetime

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
        # Ensure we're working with just the first 5 digits and pad
        zip_code = str(zip_code)[:5].zfill(5)
        return ZIP_DATA.get(zip_code, {}).get('state')
    except Exception as e:
        logger.error(f"Error looking up state for ZIP code {zip_code}: {e}")
        return None

def get_contacts_from_org_db(org_db_path: str, org_id: int) -> List[Dict[str, Any]]:
    """
    Get contacts from the organization's database
    
    Args:
        org_db_path: Path to the organization's database
        org_id: Organization ID
        
    Returns:
        List of contacts as dictionaries
    """
    logger.info(f"Getting contacts from organization database: {org_db_path}")
    
    conn = connect_to_db(org_db_path)
    try:
        cursor = conn.cursor()
        
        # Check if the contacts table exists and has the required columns
        cursor.execute("PRAGMA table_info(contacts)")
        columns = {column['name']: True for column in cursor.fetchall()}
        
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
        
        # Add state if it exists (optional - we'll derive from zip if missing)
        if 'state' in columns:
            query_parts.append('state')
        
        # Simplified WHERE clause - only check for critical data
        where_conditions = ['email IS NOT NULL AND email != ""']
        
        # Build the query
        query = f"""
            SELECT {', '.join(query_parts)} 
            FROM contacts 
            WHERE {' AND '.join(where_conditions)}
        """
        
        cursor.execute(query)
        
        contacts = []
        # Process rows in batches of 1000
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
                
            for row in rows:
                contact = dict(row)
                contact['organization_id'] = org_id
                
                # Process state from zip code if needed
                if (contact.get('state') is None or contact['state'] == '') and contact.get('zip_code'):
                    contact['state'] = get_state_from_zip(contact['zip_code'])
                
                contacts.append(contact)
            
        logger.info(f"Retrieved {len(contacts)} contacts from organization database")
        return contacts
    except sqlite3.Error as e:
        logger.error(f"Error retrieving contacts: {e}")
        raise
    finally:
        conn.close()

def get_filtered_contacts_from_org_db(org_db_path: str, org_id: int, 
                                      effective_date_age_years: Optional[int] = None,
                                      effective_date_start: Optional[str] = None,
                                      effective_date_end: Optional[str] = None,
                                      states: Optional[List[str]] = None) -> List[Dict[str, Any]]:
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
        states: Filter contacts to include only those in the specified states
        
    Returns:
        List of contacts as dictionaries
    """
    logger.info(f"Getting filtered contacts from organization database: {org_db_path}")
    
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
        
        # Add state if it exists (optional - we'll derive from zip if missing)
        if 'state' in columns:
            query_parts.append('state')
        
        # Basic WHERE clause - only check for critical data
        where_conditions = ['email IS NOT NULL AND email != ""']
        
        # Get sample dates for debugging
        cursor.execute("""
            SELECT effective_date FROM contacts 
            WHERE effective_date IS NOT NULL AND effective_date != '' 
            LIMIT 10
        """)
        sample_dates = cursor.fetchall()
        if sample_dates:
            logger.info(f"Sample effective dates in database: {[row[0] for row in sample_dates]}")
        else:
            logger.warning("No effective dates found in database!")

        # Check for exact date range filtering
        if effective_date_start and effective_date_end and 'effective_date' in columns:
            logger.info(f"Applying effective date range filter: {effective_date_start} to {effective_date_end}")
            
            # Extract year and month from the range parameters
            start_year, start_month = effective_date_start.split('-')
            end_year, end_month = effective_date_end.split('-')
            
            # Create date range condition based on different date formats
            where_conditions.append(f"""
                effective_date IS NOT NULL 
                AND effective_date != '' 
                AND (
                    -- Format YYYY-MM-DD
                    (effective_date LIKE '____-__-__' AND 
                     substr(effective_date, 1, 7) >= '{effective_date_start}' AND 
                     substr(effective_date, 1, 7) <= '{effective_date_end}')
                    
                    -- Format MM/DD/YYYY
                    OR (effective_date LIKE '__/__/____' AND 
                        (CAST(substr(effective_date, 7, 4) AS INTEGER) > {start_year} OR 
                         (CAST(substr(effective_date, 7, 4) AS INTEGER) = {start_year} AND 
                          CAST(substr(effective_date, 1, 2) AS INTEGER) >= {start_month}))
                        AND
                        (CAST(substr(effective_date, 7, 4) AS INTEGER) < {end_year} OR 
                         (CAST(substr(effective_date, 7, 4) AS INTEGER) = {end_year} AND 
                          CAST(substr(effective_date, 1, 2) AS INTEGER) <= {end_month}))
                    )
                    
                    -- Format M/D/YYYY
                    OR (effective_date LIKE '_/_/____' AND 
                        (CAST(substr(effective_date, 5, 4) AS INTEGER) > {start_year} OR 
                         (CAST(substr(effective_date, 5, 4) AS INTEGER) = {start_year} AND 
                          CAST(substr(effective_date, 1, 1) AS INTEGER) >= {start_month}))
                        AND
                        (CAST(substr(effective_date, 5, 4) AS INTEGER) < {end_year} OR 
                         (CAST(substr(effective_date, 5, 4) AS INTEGER) = {end_year} AND 
                          CAST(substr(effective_date, 1, 1) AS INTEGER) <= {end_month}))
                    )
                    
                    -- Format YYYY/MM/DD
                    OR (effective_date LIKE '____/__/__' AND 
                        substr(effective_date, 1, 7) >= '{start_year}/{start_month.zfill(2)}' AND 
                        substr(effective_date, 1, 7) <= '{end_year}/{end_month.zfill(2)}')
                )
            """)
            
        # Check for legacy effective_date_age_years parameter
        elif effective_date_age_years is not None and 'effective_date' in columns:
            # Get current year for comparison
            current_year = datetime.now().year
            
            logger.info(f"Applying effective date age filter with parameter: {effective_date_age_years}")
            
            # Handle different filtering based on parameter value
            if effective_date_age_years == 0:
                # This year - effective date in current year
                where_conditions.append(f"""
                    effective_date IS NOT NULL 
                    AND effective_date != '' 
                    AND (
                        instr(effective_date, '{current_year}') > 0 
                        OR substr(effective_date, -4) = '{current_year}'
                        OR substr(effective_date, 1, 4) = '{current_year}'
                    )
                """)
                logger.info(f"Filtering for effective dates in current year ({current_year})")
            
            elif effective_date_age_years > 0:
                # Past dates - effective date is X or more years old
                years_ago = current_year - effective_date_age_years
                where_conditions.append(f"""
                    effective_date IS NOT NULL 
                    AND effective_date != '' 
                    AND (
                        (CAST(substr(effective_date, -4) AS INTEGER) <= {years_ago} AND substr(effective_date, -4) BETWEEN '1900' AND '{years_ago}')
                        OR
                        (CAST(substr(effective_date, 1, 4) AS INTEGER) <= {years_ago} AND substr(effective_date, 1, 4) BETWEEN '1900' AND '{years_ago}')
                        OR 
                        (instr(effective_date, '/') > 0 AND 
                         CAST(substr(effective_date, instr(effective_date, '/') + 1, 4) AS INTEGER) <= {years_ago} AND 
                         substr(effective_date, instr(effective_date, '/') + 1, 4) BETWEEN '1900' AND '{years_ago}')
                    )
                """)
                logger.info(f"Filtering for effective dates {effective_date_age_years}+ years old (before or in {years_ago})")
            
            elif effective_date_age_years < 0:
                # Future dates - effective date is within next X years
                future_year = current_year + abs(effective_date_age_years)
                where_conditions.append(f"""
                    effective_date IS NOT NULL 
                    AND effective_date != '' 
                    AND (
                        (CAST(substr(effective_date, -4) AS INTEGER) >= {current_year} AND substr(effective_date, -4) BETWEEN '{current_year}' AND '{future_year}')
                        OR
                        (CAST(substr(effective_date, 1, 4) AS INTEGER) >= {current_year} AND substr(effective_date, 1, 4) BETWEEN '{current_year}' AND '{future_year}')
                        OR 
                        (instr(effective_date, '/') > 0 AND 
                         CAST(substr(effective_date, instr(effective_date, '/') + 1, 4) AS INTEGER) >= {current_year} AND 
                         CAST(substr(effective_date, instr(effective_date, '/') + 1, 4) AS INTEGER) <= {future_year})
                    )
                """)
                logger.info(f"Filtering for effective dates within next {abs(effective_date_age_years)} years (between {current_year} and {future_year})")
        
        # Add states filter if provided
        if states and len(states) > 0 and 'state' in columns:
            placeholders = ','.join(['?' for _ in states])
            where_conditions.append(f"state IN ({placeholders})")
            logger.info(f"Filtering by states: {states}")
        
        # Build the query
        query = f"""
            SELECT {', '.join(query_parts)} 
            FROM contacts 
            WHERE {' AND '.join(where_conditions)}
        """
        
        logger.debug(f"Executing SQL query: {query}")
        
        # Execute with or without state parameters
        if states and len(states) > 0 and 'state' in columns:
            cursor.execute(query, states)
        else:
            cursor.execute(query)
        
        contacts = []
        # Process rows in batches of 1000
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
                
            for row in rows:
                contact = dict(row)
                contact['organization_id'] = org_id
                
                # Process state from zip code if needed
                if (contact.get('state') is None or contact['state'] == '') and contact.get('zip_code'):
                    contact['state'] = get_state_from_zip(contact['zip_code'])
                
                contacts.append(contact)
            
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
        
        # Default to CA if we still don't have a valid state
        if not state:
            logger.error(f"Could not determine valid state for contact {contact.get('id')}")
            raise ValueError(f"Missing valid state for contact {contact.get('id')}")
        
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
                if not isinstance(formatted_contact[date_field], date):
                    if isinstance(formatted_contact[date_field], str):
                        parsed_date = parse_date_flexible(formatted_contact[date_field])
                        if parsed_date:
                            formatted_contact[date_field] = parsed_date.isoformat()
                        else:
                            logger.warning(f"Could not parse {date_field} for contact {formatted_contact['id']}: {formatted_contact[date_field]}")
                            formatted_contact[date_field] = None
                    else:
                        formatted_contact[date_field] = formatted_contact[date_field].isoformat()
                
        formatted_contacts.append(formatted_contact)
        
    logger.info(f"Formatted {len(formatted_contacts)} contacts for scheduling")
    return formatted_contacts