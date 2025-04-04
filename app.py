from fastapi import FastAPI, Request, Form, Body, Query
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
import pandas as pd
import tempfile
import os
from datetime import date, datetime, timedelta
import asyncio
from typing import Optional, List, Dict, Any
import random
import json
import logging
import os
from utils import generate_link
from email_template_engine import EmailTemplateEngine
import aiosqlite
import sqlite3

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Environment variables to control email sending
# TEST_EMAIL_SENDING controls sending real emails in test mode
# PRODUCTION_EMAIL_SENDING controls sending real emails in production mode

# Test mode email sending (default: ENABLED - can send real emails in test mode)
TEST_EMAIL_SENDING = os.environ.get("TEST_EMAIL_SENDING", "ENABLED").upper() == "ENABLED"

# Production mode email sending (default: DISABLED - won't send real emails in production)
PRODUCTION_EMAIL_SENDING = os.environ.get("PRODUCTION_EMAIL_SENDING", "DISABLED").upper() == "ENABLED"

if TEST_EMAIL_SENDING:
    logger.info("Test email sending is ENABLED - test emails will be sent to test addresses")
else:
    logger.info("Test email sending is DISABLED - no emails will be sent in test mode")

if PRODUCTION_EMAIL_SENDING:
    logger.warning("PRODUCTION email sending is ENABLED! Real emails will be sent to actual recipients!")
else:
    logger.info("Production email sending is DISABLED - no emails will be sent to actual recipients")

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle dates and other special types"""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, type):
            return str(obj)
        if callable(obj):
            return str(obj)
        return super().default(obj)

# Import our updated email scheduling code
from email_scheduler_optimized import (
    EmailScheduler,
    AsyncEmailProcessor,
    main_async,
    main_sync
)

from contact_rule_engine import ContactRuleEngine
from email_scheduler_common import (
    ALL_STATES,
)

# Import our database and formatting functions
from org_utils import (
    get_organization_details,
    get_n_contacts_from_org_db,
    format_contact_data,
    get_filtered_contacts_from_org_db,
    update_all_org_dbs_states,
    get_contacts_from_org_db,
)

# Database paths setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
main_db = os.path.join(BASE_DIR, "main.db")
org_db_dir = os.path.join(BASE_DIR, "org_dbs")

# Create org_dbs directory if it doesn't exist
os.makedirs(org_db_dir, exist_ok=True)

reload_db = False # set to True to refresh the database

async def refresh_databases(org_id: int) -> None:
    """
    Refresh databases by running dump_and_convert.sh script if reload is enabled
    
    Args:
        org_id: Organization ID to refresh
    """
    if reload_db:
        # Run dump_and_convert.sh with the org ID
        print(f"Refreshing database for org {org_id}")
        process = await asyncio.create_subprocess_shell(
            f"./dump_and_convert.sh {org_id}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for process to complete
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            # Log error but continue
            print(f"Warning: Database refresh failed for org {org_id}")
            if stderr:
                print(f"Error: {stderr.decode()}")
    else:
        print(f"Skipping database refresh for org {org_id}")

app = FastAPI(title="Email Schedule Checker")

@app.on_event("startup")
async def startup_event():
    """Run database updates when the application starts"""
    logger.info("Running startup tasks...")
    try:
        # Update states in all organization databases
        update_all_org_dbs_states()
        logger.info("Successfully updated states in all organization databases")
    except Exception as e:
        logger.error(f"Error during startup state update: {e}")
        # Don't raise the exception - allow the app to start even if this fails

# Set up Jinja2 templates
templates = Jinja2Templates(directory="templates")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Store DataFrames in memory (key: org_id)
org_data_store = {}

# Initialize our rule engine and scheduler
rule_engine = ContactRuleEngine()
email_scheduler = EmailScheduler()

# Get list of states with special rules from rule engine
SPECIAL_RULE_STATES = sorted(rule_engine.get_special_rule_states())

# Get state rules
BIRTHDAY_RULE_STATES = {state: rule_engine.get_state_rule(state) for state in rule_engine.state_rules if rule_engine.get_state_rule(state).get('type') == 'birthday'}
EFFECTIVE_DATE_RULE_STATES = {state: rule_engine.get_state_rule(state) for state in rule_engine.state_rules if rule_engine.get_state_rule(state).get('type') == 'effective_date'}
YEAR_ROUND_ENROLLMENT_STATES = [state for state in rule_engine.state_rules if rule_engine.is_year_round_enrollment_state(state)]

def get_all_occurrences(base_date: date, start_date: date, end_date: date) -> List[date]:
    """
    Get all yearly occurrences of a date between start_date and end_date
    
    Args:
        base_date: The reference date
        start_date: Start of the range
        end_date: End of the range
        
    Returns:
        List of dates for each yearly occurrence
    """
    if not base_date or not start_date or not end_date:
        return []
        
    dates = []
    current_year = start_date.year
    while current_year <= end_date.year:
        try:
            yearly_date = date(current_year, base_date.month, base_date.day)
            if start_date <= yearly_date <= end_date:
                dates.append(yearly_date)
        except ValueError:
            # Handle Feb 29 in non-leap years
            if base_date.month == 2 and base_date.day == 29:
                yearly_date = date(current_year, 2, 28)
                if start_date <= yearly_date <= end_date:
                    dates.append(yearly_date)
        current_year += 1
    return dates

def sample_contacts_from_states(unique_contacts: pd.DataFrame, sample_size: int, state: Optional[str] = None) -> List[str]:
    """
    Sample contacts ensuring a good distribution across states
    
    Args:
        unique_contacts: DataFrame of unique contacts with their states
        sample_size: Number of contacts to sample
        state: Optional specific state to filter by
        
    Returns:
        List of sampled contact IDs
    """
    sample_ids = []
    
    # If filtering by specific state, do simple random sample
    if state and state.strip():
        state_contacts = unique_contacts[unique_contacts['state'] == state]
        if len(state_contacts) > 0:
            sample_ids = random.sample(
                list(state_contacts['contact_id']), 
                min(sample_size, len(state_contacts))
            )
        return sample_ids
    
    # Get contacts grouped by state
    states_contacts = {
        state: group['contact_id'].tolist() 
        for state, group in unique_contacts.groupby('state')
    }
    
    # If we have fewer states than sample size, adjust distribution
    states_count = len(states_contacts)
    if states_count == 0:
        return []
    
    # Calculate initial distribution
    if states_count >= sample_size:
        # If we have more states than sample size, randomly select states
        selected_states = random.sample(list(states_contacts.keys()), sample_size)
        # Take one contact from each selected state
        for state in selected_states:
            if states_contacts[state]:
                contact = random.choice(states_contacts[state])
                sample_ids.append(contact)
    else:
        # Distribute samples across states as evenly as possible
        base_per_state = sample_size // states_count
        extra = sample_size % states_count
        
        # Shuffle states to randomize which ones get extra samples
        state_list = list(states_contacts.keys())
        random.shuffle(state_list)
        
        # Distribute samples
        for i, state in enumerate(state_list):
            # Calculate how many samples for this state
            state_sample_size = base_per_state + (1 if i < extra else 0)
            state_contacts = states_contacts[state]
            
            # If we don't have enough contacts in this state, take what we can
            state_sample_size = min(state_sample_size, len(state_contacts))
            
            if state_sample_size > 0:
                state_samples = random.sample(state_contacts, state_sample_size)
                sample_ids.extend(state_samples)
    
    # If we still need more samples, take them randomly from remaining contacts
    if len(sample_ids) < sample_size:
        remaining_contacts = [
            cid for cid in unique_contacts['contact_id'] 
            if cid not in sample_ids
        ]
        if remaining_contacts:
            additional_needed = sample_size - len(sample_ids)
            additional_samples = random.sample(
                remaining_contacts,
                min(additional_needed, len(remaining_contacts))
            )
            sample_ids.extend(additional_samples)
    
    return sample_ids

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the home page with organization input form"""
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "title": "Email Schedule Checker",
            "sample_sizes": [5, 10, 25, 50, 100],
            "all_states": ALL_STATES,
            "special_rule_states": SPECIAL_RULE_STATES,
            "state_rules": {
                state: {
                    "has_birthday_rule": state in BIRTHDAY_RULE_STATES,
                    "has_effective_date_rule": state in EFFECTIVE_DATE_RULE_STATES,
                    "has_year_round_enrollment": state in YEAR_ROUND_ENROLLMENT_STATES
                }
                for state in ALL_STATES
            }
        }
    )

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Display a live dashboard of scheduled emails by state"""
    # Initialize data counters
    email_counts_by_state = {state: {
        "birthday": 0,
        "effective_date": 0,
        "aep": 0,
        "post_window": 0,
        "total": 0,
        "skipped": 0,
        "has_special_rule": state in SPECIAL_RULE_STATES
    } for state in ALL_STATES}
    
    # Placeholder data - in a real app, this would come from database
    total_emails = 0
    total_skipped = 0
    
    # Add dummy data for demonstration
    for state in SPECIAL_RULE_STATES:
        email_counts_by_state[state]["birthday"] = random.randint(10, 50)
        email_counts_by_state[state]["effective_date"] = random.randint(5, 40)
        email_counts_by_state[state]["aep"] = random.randint(0, 30) if state not in YEAR_ROUND_ENROLLMENT_STATES else 0
        email_counts_by_state[state]["post_window"] = random.randint(0, 20) if state not in YEAR_ROUND_ENROLLMENT_STATES else 0
        email_counts_by_state[state]["skipped"] = random.randint(1, 10)
        email_counts_by_state[state]["total"] = (
            email_counts_by_state[state]["birthday"] + 
            email_counts_by_state[state]["effective_date"] + 
            email_counts_by_state[state]["aep"] + 
            email_counts_by_state[state]["post_window"]
        )
        
        total_emails += email_counts_by_state[state]["total"]
        total_skipped += email_counts_by_state[state]["skipped"]
    
    # Add some data for non-special states
    for i, state in enumerate(list(set(ALL_STATES) - set(SPECIAL_RULE_STATES))):
        if i < 10:  # Only populate some non-special states
            email_counts_by_state[state]["birthday"] = random.randint(5, 30)
            email_counts_by_state[state]["effective_date"] = random.randint(3, 25)
            email_counts_by_state[state]["aep"] = random.randint(0, 20)
            email_counts_by_state[state]["post_window"] = random.randint(0, 15)
            email_counts_by_state[state]["skipped"] = random.randint(0, 5)
            email_counts_by_state[state]["total"] = (
                email_counts_by_state[state]["birthday"] + 
                email_counts_by_state[state]["effective_date"] + 
                email_counts_by_state[state]["aep"] + 
                email_counts_by_state[state]["post_window"]
            )
            
            total_emails += email_counts_by_state[state]["total"]
            total_skipped += email_counts_by_state[state]["skipped"]
    
    # Calculate percentages for total stats
    email_type_totals = {
        "birthday": sum(state_data["birthday"] for state_data in email_counts_by_state.values()),
        "effective_date": sum(state_data["effective_date"] for state_data in email_counts_by_state.values()),
        "aep": sum(state_data["aep"] for state_data in email_counts_by_state.values()),
        "post_window": sum(state_data["post_window"] for state_data in email_counts_by_state.values()),
    }
    
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "title": "Email Scheduling Dashboard",
            "email_counts": email_counts_by_state,
            "total_emails": total_emails,
            "total_skipped": total_skipped,
            "email_type_totals": email_type_totals,
            "all_states": ALL_STATES,
            "special_rule_states": SPECIAL_RULE_STATES,
            "birthday_rule_states": BIRTHDAY_RULE_STATES,
            "effective_date_rule_states": EFFECTIVE_DATE_RULE_STATES,
            "year_round_enrollment_states": YEAR_ROUND_ENROLLMENT_STATES
        }
    )

# Simulator data model
class SimulationRequest(BaseModel):
    state: str
    birth_date: str
    effective_date: Optional[str] = None
    start_date: str
    end_date: str

@app.get("/simulator", response_class=HTMLResponse)
async def simulator(request: Request):
    """Display email scheduling simulator"""
    # Default dates
    today = date.today()
    next_year = today + timedelta(days=365)
    
    return templates.TemplateResponse(
        "simulator.html",
        {
            "request": request,
            "title": "Email Scheduler Simulator",
            "all_states": ALL_STATES,
            "special_rule_states": SPECIAL_RULE_STATES,
            "birthday_rule_states": BIRTHDAY_RULE_STATES,
            "effective_date_rule_states": EFFECTIVE_DATE_RULE_STATES,
            "year_round_enrollment_states": YEAR_ROUND_ENROLLMENT_STATES,
            "today": today.isoformat(),
            "next_year": next_year.isoformat()
        }
    )

@app.post("/simulate")
async def simulate_emails(data: SimulationRequest):
    """Simulate email scheduling for a given contact"""
    try:
        # Parse dates - ensure we're working with date objects
        try:
            birth_date = datetime.strptime(data.birth_date, "%Y-%m-%d").date() if isinstance(data.birth_date, str) else data.birth_date
            start_date = datetime.strptime(data.start_date, "%Y-%m-%d").date() if isinstance(data.start_date, str) else data.start_date
            end_date = datetime.strptime(data.end_date, "%Y-%m-%d").date() if isinstance(data.end_date, str) else data.end_date
            
            effective_date = None
            if data.effective_date:
                effective_date = datetime.strptime(data.effective_date, "%Y-%m-%d").date() if isinstance(data.effective_date, str) else data.effective_date
        except (TypeError, ValueError) as e:
            logger.error(f"Date parsing error: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid date format. Please use YYYY-MM-DD format."}
            )
            
        # Set up contact data with dates as strings for the scheduler
        contact = {
            "id": "12345",  # Dummy ID
            "birth_date": birth_date.isoformat() if birth_date else None,
            "effective_date": effective_date.isoformat() if effective_date else None,
            "state": data.state,
            "first_name": "Test",
            "last_name": "Contact",
            "email": "test@example.com"
        }
        
        logger.debug("Processing contact with birth_date=%s, effective_date=%s", 
                    contact["birth_date"], contact["effective_date"])
        
        # Use the scheduler to process the contact
        try:
            result = email_scheduler.process_contact(contact, start_date, end_date)
            if not result:
                return JSONResponse(
                    status_code=400,
                    content={"error": "No results returned from email scheduler"}
                )
            logger.debug("Scheduler result: %s", result)  # Add debug logging
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={"error": f"Error processing contact: {str(e)}"}
            )
        
        # Format the result for the response
        email_list = []
        
        # Add scheduled emails
        for email in result.get("scheduled", []):
            try:
                email_date = email.get('scheduled_date') or email.get('date')
                if not email_date:
                    logger.warning(f"No date found in email data: {email}")
                    continue
                    
                email_info = {
                    'type': email.get('type', 'unknown'),
                    'type_display': {
                        'birthday': 'Birthday Email',
                        'anniversary': 'Anniversary Email',
                        'aep': 'AEP Email',
                        'post_window': 'Post-Window Email'
                    }.get(email.get('type', 'unknown'), email.get('type', 'unknown').replace('_', ' ').title()),
                    'start': email_date.isoformat() if isinstance(email_date, date) else email_date,
                    'skipped': False,
                    'reason': '',
                    'link': f"/contact/12345/email/{email.get('type', 'unknown')}/{email_date}",
                    'default_date': None
                }
                
                # Set default dates based on type
                if email.get('type') == 'birthday':
                    email_info['default_date'] = birth_date.isoformat()
                elif email.get('type') == 'anniversary':
                    email_info['default_date'] = effective_date.isoformat() if effective_date else None
                elif email.get('type') == 'aep':
                    email_info['default_date'] = 'AEP Window'
                elif email.get('type') == 'post_window':
                    email_info['default_date'] = 'Post Exclusion Period'
                    
                email_list.append(email_info)
            except Exception as e:
                logger.error(f"Error processing scheduled email: {e}\nEmail data: {email}")
                continue
            
        # Add skipped emails
        for email in result.get("skipped", []):
            try:
                email_date = email.get('scheduled_date') or email.get('date')
                if not email_date:
                    logger.warning(f"No date found in skipped email data: {email}")
                    continue
                    
                email_info = {
                    'type': email.get('type', 'unknown'),
                    'type_display': {
                        'birthday': 'Birthday Email',
                        'anniversary': 'Anniversary Email',
                        'aep': 'AEP Email',
                        'post_window': 'Post-Window Email'
                    }.get(email.get('type', 'unknown'), email.get('type', 'unknown').replace('_', ' ').title()),
                    'start': email_date.isoformat() if isinstance(email_date, date) else email_date,
                    'skipped': True,
                    'reason': email.get('reason', 'No reason provided'),
                    'link': '',
                    'default_date': None
                }
                
                # Set default dates based on type
                if email.get('type') == 'birthday':
                    email_info['default_date'] = birth_date.isoformat()
                elif email.get('type') == 'anniversary':
                    email_info['default_date'] = effective_date.isoformat() if effective_date else None
                elif email.get('type') == 'aep':
                    email_info['default_date'] = 'AEP Window'
                elif email.get('type') == 'post_window':
                    email_info['default_date'] = 'Post Exclusion Period'
                    
                email_list.append(email_info)
            except Exception as e:
                logger.error(f"Error processing skipped email: {e}\nEmail data: {email}")
                continue
        
        # Sort email list by date
        email_list.sort(key=lambda x: x['start'])
        
        # Get state rules
        rules = []
        if data.state in BIRTHDAY_RULE_STATES:
            window = BIRTHDAY_RULE_STATES[data.state]
            rules.append(f"Birthday emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after birthday")
        if data.state in EFFECTIVE_DATE_RULE_STATES:
            window = EFFECTIVE_DATE_RULE_STATES[data.state]
            rules.append(f"Effective date emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after anniversary")
        if data.state in YEAR_ROUND_ENROLLMENT_STATES:
            rules.append("Year-round enrollment state - no scheduled emails")
        else:
            rules.append("AEP emails: Distributed across August/September")
            rules.append("Post-window emails: Day after exclusion period")
        
        response = {
            "contact_info": {
                "id": "12345",
                "name": "Test Contact",
                "email": "test@example.com",
                "state": data.state,
                "birth_date": birth_date.isoformat(),
                "effective_date": effective_date.isoformat() if effective_date else None
            },
            "timeline_data": {
                "email_list": email_list
            },
            "scheduling_rules": rules,
            "state_info": {
                "code": data.state,
                "has_birthday_rule": data.state in BIRTHDAY_RULE_STATES,
                "has_effective_date_rule": data.state in EFFECTIVE_DATE_RULE_STATES,
                "has_year_round_enrollment": data.state in YEAR_ROUND_ENROLLMENT_STATES
            }
        }
        
        return response
        
    except Exception as e:
        # Log the error and return an error response
        import traceback
        error_trace = traceback.format_exc()
        logger.error("Error in simulate_emails: %s\n%s", str(e), error_trace)
        return JSONResponse(
            status_code=500,
            content={"error": f"An error occurred while calculating scheduled emails: {str(e)}"}
        )

@app.post("/resample/{org_id}")
async def resample_contacts(
    org_id: int, 
    sample_size: int = 10,
    state: Optional[str] = None,
    special_rules_only: bool = False,
    contact_search: Optional[str] = None
):
    """Resample contacts from existing data"""
    try:
        logger.debug("Starting resample_contacts with org_id=%s, sample_size=%s, state=%s", 
                    org_id, sample_size, state)
        
        # Set date range
        current_date = date.today()
        end_date = date(current_date.year + 2, current_date.month, current_date.day)
        
        if org_id not in org_data_store:
            logger.warning("Organization data not found for org_id=%s", org_id)
            return JSONResponse(
                status_code=404,
                content={"error": "Organization data not found. Please run the initial check first."}
            )
            
        df = org_data_store[org_id]
        logger.debug("Retrieved dataframe with %d rows", len(df))
        
        # Apply contact search if provided
        if contact_search and contact_search.strip():
            search_term = contact_search.strip()
            logger.debug("Applying contact search with term: %s", search_term)
            # Search by email (case insensitive) or by contact ID
            filtered_df = df[(df['email'].str.lower() == search_term.lower()) | 
                             (df['contact_id'].astype(str) == search_term)]
            
            if len(filtered_df) == 0:
                logger.warning("No contacts found matching search term: %s", search_term)
                return JSONResponse(
                    status_code=404,
                    content={"error": f"No contact found with email or ID: {search_term}"}
                )
        else:
            # Apply state filtering
            filtered_df = df.copy()
            if special_rules_only:
                logger.debug("Filtering for special rules states")
                filtered_df = filtered_df[filtered_df['state'].isin(SPECIAL_RULE_STATES)]
            elif state and state.strip():
                logger.debug("Filtering for state: %s", state)
                filtered_df = filtered_df[filtered_df['state'] == state]
                
            # Get unique contacts with their states
            unique_contacts = filtered_df.groupby('contact_id').first().reset_index()
            logger.debug("Found %d unique contacts after filtering", len(unique_contacts))
            
            if len(unique_contacts) == 0:
                logger.warning("No contacts found after state filtering")
                return JSONResponse(
                    status_code=404,
                    content={"error": "No contacts found matching the state filter criteria."}
                )
            
            # Sample contacts ensuring good state distribution
            sample_ids = sample_contacts_from_states(unique_contacts, sample_size, state if state and state.strip() else None)
            logger.debug("Sampled %d contact IDs", len(sample_ids))
            
            # Filter dataframe to only include sampled contacts
            filtered_df = filtered_df[filtered_df['contact_id'].isin(sample_ids)]
        
        # Convert DataFrame to list of dicts, handling NaN values
        sample_data = filtered_df.replace({pd.NA: None}).to_dict('records')
        logger.debug("Converted filtered data to %d records", len(sample_data))
        
        # Group data by contact with improved organization
        contacts_data = {}
        
        # First pass: Initialize contact data and calculate dates
        for row in sample_data:
            contact_id = row['contact_id']
            if contact_id not in contacts_data:
                state_code = row['state']
                state_info = {
                    "code": state_code,
                    "has_birthday_rule": state_code in BIRTHDAY_RULE_STATES,
                    "has_effective_date_rule": state_code in EFFECTIVE_DATE_RULE_STATES,
                    "has_year_round_enrollment": state_code in YEAR_ROUND_ENROLLMENT_STATES,
                    "rule_details": {
                        "birthday": BIRTHDAY_RULE_STATES.get(state_code, {}),
                        "effective_date": EFFECTIVE_DATE_RULE_STATES.get(state_code, {})
                    }
                }
                
                # Get birth_date and effective_date
                birth_date = row.get('birth_date')
                effective_date = row.get('effective_date')
                
                logger.debug("Processing contact %s with birth_date=%s, effective_date=%s", 
                           contact_id, birth_date, effective_date)
                
                # Calculate birthdays and effective dates in range
                birthdays = []
                effective_dates = []
                if birth_date:
                    try:
                        birthdays = get_all_occurrences(pd.to_datetime(birth_date).date(), current_date, end_date)
                        logger.debug("Calculated %d birthdays for contact %s", len(birthdays), contact_id)
                    except Exception as e:
                        logger.error("Error calculating birthdays for contact %s: %s", contact_id, e)
                if effective_date:
                    try:
                        effective_dates = get_all_occurrences(pd.to_datetime(effective_date).date(), current_date, end_date)
                        logger.debug("Calculated %d effective dates for contact %s", len(effective_dates), contact_id)
                    except Exception as e:
                        logger.error("Error calculating effective dates for contact %s: %s", contact_id, e)
                
                # Initialize contact data structure
                contacts_data[contact_id] = {
                    'contact_info': {
                        'id': contact_id,
                        'name': f"{row['first_name']} {row['last_name']}",
                        'email': row['email'],
                        'state': state_code,
                        'state_info': state_info,
                        'birth_date': birth_date,
                        'effective_date': effective_date
                    },
                    'timeline_data': {
                        'groups': [
                            {'id': 'key_dates', 'content': 'Key Dates'},
                            {'id': 'emails', 'content': 'Scheduled Emails'},
                            {'id': 'windows', 'content': 'Special Windows'}
                        ],
                        'items': []
                    }
                }
                
                # Add applicable scheduling rules based on state
                rules = []
                if state_info['has_birthday_rule']:
                    window = BIRTHDAY_RULE_STATES.get(state_code, {})
                    rules.append(f"Birthday emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after birthday")
                if state_info['has_effective_date_rule']:
                    window = EFFECTIVE_DATE_RULE_STATES.get(state_code, {})
                    rules.append(f"Effective date emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after anniversary")
                if state_info['has_year_round_enrollment']:
                    rules.append("Year-round enrollment state - no scheduled emails")
                else:
                    rules.append("AEP emails: Distributed across August/September")
                    rules.append("Post-window emails: Day after exclusion period")
                contacts_data[contact_id]['scheduling_rules'] = rules

        logger.debug("Processed %d contacts into contacts_data", len(contacts_data))
        
        # Log the structure of contacts_data for one contact
        if contacts_data:
            sample_contact_id = next(iter(contacts_data))
            logger.debug("Sample contact data structure: %s", 
                        json.dumps(contacts_data[sample_contact_id], default=str))

        # Use custom encoder for response
        response_data = {
            "contacts": contacts_data,
            "total_contacts": len(df.groupby('contact_id')),
            "sample_size": len(contacts_data),
            "contact_search": contact_search if contact_search else ""
        }
        
        return JSONResponse(
            content=response_data,
            encoder=CustomJSONEncoder
        )
        
    except Exception as e:
        logger.exception("Error in resample_contacts")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

@app.get("/check", response_class=HTMLResponse)
@app.post("/check", response_class=HTMLResponse)
async def check_schedules(
    request: Request,
    org_id: Optional[int] = None,
    sample_size: Optional[int] = None,
    state: Optional[str] = None,
    special_rules_only: Optional[bool] = None,
    contact_search: Optional[str] = None,
    show_all: Optional[bool] = None,
    effective_date_filter: Optional[str] = None,
    effective_date_years: Optional[int] = None,
    effective_date_start: Optional[str] = None,
    effective_date_end: Optional[str] = None,
    page: Optional[int] = None,
    message: Optional[str] = None
):
    # Handle request method-specific parameter extraction
    if request.method == "POST":
        # For POST requests, extract form parameters
        form_data = await request.form()
        org_id = int(form_data.get("org_id"))
        sample_size = int(form_data.get("sample_size", "10"))
        state = form_data.get("state")
        special_rules_only = form_data.get("special_rules_only") == "true"
        contact_search = form_data.get("contact_search")
        show_all = form_data.get("show_all") == "true"
        effective_date_filter = form_data.get("effective_date_filter", "none")
        page = int(form_data.get("page", "1"))
        
        # Handle optional numeric parameters
        if "effective_date_years" in form_data:
            effective_date_years = int(form_data.get("effective_date_years"))
        if "effective_date_start" in form_data:
            effective_date_start = form_data.get("effective_date_start")
        if "effective_date_end" in form_data:
            effective_date_end = form_data.get("effective_date_end")
    else:
        # For GET requests, extract parameters from query
        params = dict(request.query_params)
        org_id = int(params.get("org_id", "0"))
        sample_size = int(params.get("sample_size", "10"))
        state = params.get("state")
        special_rules_only = params.get("special_rules_only", "").lower() in ["true", "1", "yes"]
        contact_search = params.get("contact_search")
        show_all = params.get("show_all", "").lower() in ["true", "1", "yes"]  
        effective_date_filter = params.get("effective_date_filter", "none")
        page = int(params.get("page", "1"))
        message = params.get("message")
        
        # Convert effective date parameters if present
        if "effective_date_years" in params:
            effective_date_years = int(params.get("effective_date_years"))
        if "effective_date_start" in params:
            effective_date_start = params.get("effective_date_start")
        if "effective_date_end" in params:
            effective_date_end = params.get("effective_date_end")

    try:
        # Convert effective date values to integers, handling -1 case
        effective_date_start_int = None
        effective_date_end_int = None
        
        if effective_date_start is not None:
            try:
                effective_date_start_int = int(effective_date_start)
            except ValueError:
                logger.error(f"Invalid effective_date_start value: {effective_date_start}")
                return templates.TemplateResponse(
                    "error.html",
                    {
                        "request": request,
                        "error": "Invalid start date value provided"
                    }
                )
                
        if effective_date_end is not None:
            try:
                effective_date_end_int = int(effective_date_end)
            except ValueError:
                logger.error(f"Invalid effective_date_end value: {effective_date_end}")
                return templates.TemplateResponse(
                    "error.html",
                    {
                        "request": request,
                        "error": "Invalid end date value provided"
                    }
                )

        # Set date range
        current_date = date.today()
        end_date = date(current_date.year + 2, current_date.month, current_date.day)

        await refresh_databases(org_id)
        
        # Get organization details
        org = get_organization_details(main_db, org_id)
        org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
        
        # Calculate effective date range if filter is active
        effective_date_age_years = None
        effective_date_range_start = None
        effective_date_range_end = None
        
        if effective_date_filter == "single" and effective_date_years:
            effective_date_age_years = effective_date_years
        elif effective_date_filter == "range" and effective_date_start_int is not None:
            # Calculate date range based on first day of current month
            today = date.today()
            first_of_month = date(today.year, today.month, 1)
            
            # Handle start date (this will be the earlier/older date)
            if effective_date_start_int == -1:
                # No start limit
                effective_date_range_start = None
            else:
                # Calculate start date (earlier/older date)
                effective_date_range_start = first_of_month - timedelta(days=effective_date_start_int * 30)  # Using 30 days per month for consistency
                effective_date_range_start = effective_date_range_start.strftime("%Y-%m")
            
            # Handle end date (this will be the later/newer date)
            if effective_date_end_int is not None and effective_date_end_int != -1:
                # Calculate end date (later/newer date)
                effective_date_range_end = first_of_month - timedelta(days=effective_date_end_int * 30)
                effective_date_range_end = effective_date_range_end.strftime("%Y-%m")
            else:
                # No end limit
                effective_date_range_end = None
                
            # Swap start and end dates if needed (since larger months-ago number means earlier date)
            if (effective_date_range_start is not None and effective_date_range_end is not None and 
                effective_date_range_start < effective_date_range_end):
                effective_date_range_start, effective_date_range_end = effective_date_range_end, effective_date_range_start
        
        # Determine states to filter by
        states_to_filter = None
        if special_rules_only:
            states_to_filter = SPECIAL_RULE_STATES
            logger.debug(f"Filtering by special rules states: {states_to_filter}")
        elif state and state.strip():
            states_to_filter = [state]
            logger.debug(f"Filtering by specific state: {states_to_filter}")

        # First, get the filtered universe of contacts based on criteria
        logger.debug(f"Getting filtered universe with states={states_to_filter}, effective_date_start={effective_date_range_start}, effective_date_end={effective_date_range_end}")
        filtered_contacts = get_filtered_contacts_from_org_db(
            org_db_path, 
            org_id,
            states=states_to_filter,
            n=None,  # No limit when getting universe
            is_random=False,  # No randomization when getting universe
            effective_date_age_years=effective_date_age_years,
            effective_date_start=effective_date_range_start,
            effective_date_end=effective_date_range_end
        )
        
        # If searching for a specific contact, filter the universe further
        if contact_search and contact_search.strip():
            logger.debug(f"Filtering universe for contact search: {contact_search}")
            search_term = contact_search.strip().lower()
            filtered_contacts = [
                contact for contact in filtered_contacts 
                if search_term in contact.get('email', '').lower() or 
                str(contact.get('id', '')) == search_term
            ]
            
            if not filtered_contacts:
                return templates.TemplateResponse(
                    "error.html",
                    {
                        "request": request,
                        "error": f"No contact found with email or ID: {contact_search}"
                    }
                )
        
        # Now handle sampling from the filtered universe if not showing all
        if not show_all and not contact_search:
            logger.debug(f"Sampling {sample_size} contacts from universe of {len(filtered_contacts)}")
            if len(filtered_contacts) > sample_size:
                filtered_contacts = random.sample(filtered_contacts, sample_size)
            
        logger.debug(f"Processing {len(filtered_contacts)} contacts")
        formatted_contacts = format_contact_data(filtered_contacts)

        if not formatted_contacts:
            error_msg = "No valid contacts found for scheduling"
            if state:
                error_msg += f" in state {state}"
            elif special_rules_only:
                error_msg += " in states with special rules"
            if effective_date_filter != "none":
                error_msg += " with the specified effective date filter"
            logger.warning(error_msg)
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": error_msg
                }
            )

        # Handle pagination when showing all contacts
        total_contacts = len(formatted_contacts)
        contacts_per_page = 50  # Number of contacts to show per page
        total_pages = (total_contacts + contacts_per_page - 1) // contacts_per_page if show_all else 1
        
        if show_all and total_contacts > contacts_per_page:
            # Calculate pagination
            page = max(1, min(page, total_pages))  # Ensure page is within valid range
            start_idx = (page - 1) * contacts_per_page
            end_idx = min(start_idx + contacts_per_page, total_contacts)
            
            # Slice contacts for current page
            current_contacts = formatted_contacts[start_idx:end_idx]
            has_previous = page > 1
            has_next = page < total_pages
        else:
            current_contacts = formatted_contacts
            page = 1
            has_previous = False
            has_next = False
            total_pages = 1

        # Process contacts using the simplified async approach
        try:
            if len(current_contacts) < 100:
                results = main_sync(current_contacts, current_date, end_date)
            else:
                results = await main_async(current_contacts, current_date, end_date, batch_size=min(len(current_contacts) // 10, 1000))
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Failed to process contacts: {str(e)}\n{error_trace}")
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to process contacts: {str(e)}\nTrace:\n{error_trace}"
                }
            )

        # Convert results to DataFrame for easier filtering and organization
        df_data = []
        for result in results:
            contact_id = result['contact_id']
            contact = next((c for c in current_contacts if c['id'] == contact_id), None)
            if contact:
                # Add scheduled emails
                for email in result.get('scheduled', []):
                    df_data.append({
                        'contact_id': contact_id,
                        'first_name': contact.get('first_name', ''),
                        'last_name': contact.get('last_name', ''),
                        'email': contact.get('email', ''),
                        'state': contact.get('state', ''),
                        'birth_date': contact.get('birth_date'),
                        'effective_date': contact.get('effective_date'),
                        'email_type': email['type'],
                        'email_date': email['date'],
                        'skipped': 'No',
                        'reason': '',
                        'link': f"/contact/{contact_id}/email/{email['type']}/{email['date']}"
                    })
                
                # Add skipped emails
                for email in result.get('skipped', []):
                    df_data.append({
                        'contact_id': contact_id,
                        'first_name': contact.get('first_name', ''),
                        'last_name': contact.get('last_name', ''),
                        'email': contact.get('email', ''),
                        'state': contact.get('state', ''),
                        'birth_date': contact.get('birth_date'),
                        'effective_date': contact.get('effective_date'),
                        'email_type': email['type'],
                        'email_date': email.get('date', current_date),
                        'skipped': 'Yes',
                        'reason': email.get('reason', ''),
                        'link': ''
                    })

        # Create DataFrame and store in memory
        df = pd.DataFrame(df_data)
        org_data_store[org_id] = df

        # Filter contacts if searching
        if contact_search and contact_search.strip():
            search_term = contact_search.strip()
            filtered_df = df[(df['email'].str.lower() == search_term.lower()) | 
                             (df['contact_id'].astype(str) == search_term)]
            
            if len(filtered_df) == 0:
                return templates.TemplateResponse(
                    "error.html",
                    {
                        "request": request,
                        "error": f"No contact found with email or ID: {search_term}"
                    }
                )
            df = filtered_df

        # Prepare contact data for display
        contacts_data = {}
        for contact_id in df['contact_id'].unique():
            contact_rows = df[df['contact_id'] == contact_id]
            if len(contact_rows) == 0:
                continue

            first_row = contact_rows.iloc[0]
            state_code = first_row['state']
            
            # Initialize contact data structure
            contacts_data[contact_id] = {
                'contact_info': {
                    'id': contact_id,
                    'name': f"{first_row['first_name']} {first_row['last_name']}",
                    'email': first_row['email'],
                    'state': state_code,
                    'birth_date': first_row['birth_date'],
                    'effective_date': first_row['effective_date']
                },
                'timeline_data': {
                    'email_list': []
                }
            }

            # Add applicable scheduling rules based on state
            rules = []
            if state_code in BIRTHDAY_RULE_STATES:
                window = BIRTHDAY_RULE_STATES[state_code]
                rules.append(f"Birthday emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after birthday")
            if state_code in EFFECTIVE_DATE_RULE_STATES:
                window = EFFECTIVE_DATE_RULE_STATES[state_code]
                rules.append(f"Effective date emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after anniversary")
            if state_code in YEAR_ROUND_ENROLLMENT_STATES:
                rules.append("Year-round enrollment state - no scheduled emails")
            else:
                rules.append("AEP emails: Distributed across August/September")
                rules.append("Post-window emails: Day after exclusion period")
            contacts_data[contact_id]['scheduling_rules'] = rules

            # Add emails to timeline data
            email_list = []
            for _, row in contact_rows.iterrows():
                email_type = row['email_type']
                # Map email types to human-readable names and determine default dates
                email_info = {
                    'type': email_type,
                    'type_display': {
                        'birthday': 'Birthday Email',
                        'anniversary': 'Anniversary Email',
                        'aep': 'AEP Email',
                        'post_window': 'Post-Window Email'
                    }.get(email_type, email_type.replace('_', ' ').title()),
                    'start': row['email_date'],
                    'skipped': row['skipped'] == 'Yes',
                    'reason': row['reason'] if row['skipped'] == 'Yes' else '',
                    'link': row['link'],
                    'default_date': None  # Will be populated based on type
                }
                
                # Set default dates based on type
                if email_type == 'birthday' and first_row['birth_date']:
                    email_info['default_date'] = first_row['birth_date']
                elif email_type == 'anniversary' and first_row['effective_date']:
                    email_info['default_date'] = first_row['effective_date']
                elif email_type == 'aep':
                    email_info['default_date'] = 'AEP Window'
                elif email_type == 'post_window':
                    email_info['default_date'] = 'Post Exclusion Period'
                
                email_list.append(email_info)
            
            # Sort email list by date
            email_list.sort(key=lambda x: x['start'])
            contacts_data[contact_id]['timeline_data']['email_list'] = email_list

        # Return the rendered template with processed data
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "contacts": list(contacts_data.values()),
                "org_id": org_id,
                "org_name": org.get('name', f'Organization {org_id}'),
                "sample_size": len(contacts_data),
                "total_contacts": total_contacts,
                "sample_sizes": [10, 25, 50, 100, 250, 500],
                "contact_search": contact_search or "",
                "generate_link": generate_link,
                "show_all": show_all,
                "effective_date_filter": effective_date_filter,
                "effective_date_years": effective_date_years,
                "effective_date_start": effective_date_start,
                "effective_date_end": effective_date_end,
                "current_page": page,
                "total_pages": total_pages,
                "has_previous": has_previous,
                "has_next": has_next,
                "message": message
            }
        )
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Unexpected error in check_schedules: {str(e)}\n{error_trace}")
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error": f"Unexpected error: {str(e)}\nTrace:\n{error_trace}"
            }
        )

@app.post("/get_universe_contacts")
async def get_universe_contacts(
    org_id: int = Form(...),
    effective_date_age_years: Optional[int] = Form(None),
    effective_date_start: Optional[str] = Form(None),
    effective_date_end: Optional[str] = Form(None),
    states: List[str] = Form([]),
    n: Optional[int] = Form(None),
    is_random: bool = Form(False)
):
    """
    Fetch filtered contacts based on effective date range and states
    
    Args:
        org_id: Organization ID
        effective_date_age_years: Legacy filter for effective date age
        effective_date_start: Start of effective date range (YYYY-MM)
        effective_date_end: End of effective date range (YYYY-MM)
        states: List of states to filter by
        n: Optional limit on number of results
        is_random: If True and n is provided, randomly sample n results
    """
    try:
        logger.debug("Starting get_universe_contacts with org_id=%s, effective_date_range=%s to %s, states=%s, n=%s, is_random=%s", 
                    org_id, effective_date_start, effective_date_end, states, n, is_random)
        
        # Get organization details and construct DB path
        org = get_organization_details(main_db, org_id)
        org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
        
        # Call the filtered contacts function with either legacy or new range parameters
        contacts = get_filtered_contacts_from_org_db(
            org_db_path, 
            org_id, 
            effective_date_age_years=effective_date_age_years if not (effective_date_start and effective_date_end) else None,
            effective_date_start=effective_date_start,
            effective_date_end=effective_date_end,
            states=states if states else None,
            n=n,
            is_random=is_random
        )
        
        # Format the contacts for the frontend
        formatted_contacts = format_contact_data(contacts)
        
        # Pre-encode content with custom encoder, then return as JSONResponse
        content = {
            "contacts": formatted_contacts,
            "total": len(formatted_contacts),
            "org_name": org['name'],
            "is_random_sample": is_random and n is not None
        }
        json_content = json.dumps(content, cls=CustomJSONEncoder)
        
        return JSONResponse(content=json.loads(json_content))
    except Exception as e:
        logger.exception("Error in get_universe_contacts")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

@app.post("/process_universe")
async def process_universe(
    request: Request,
    org_id: int = Form(...),
    contact_ids: List[str] = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...)
):
    """Process selected contacts for email scheduling"""
    try:
        logger.debug("Starting process_universe with org_id=%s, contact_ids=%s", 
                    org_id, contact_ids)
        
        # Parse dates
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid date format. Please use YYYY-MM-DD format."}
            )
        
        # Get organization details and refresh database if needed
        await refresh_databases(org_id)
        org = get_organization_details(main_db, org_id)
        org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
        
        # Get all contacts for the organization
        contacts = get_contacts_from_org_db(org_db_path, org_id)
        formatted_contacts = format_contact_data(contacts)
        
        # Filter contacts to only include those with IDs in the contact_ids list
        selected_contacts = [c for c in formatted_contacts if str(c['id']) in contact_ids]
        
        if not selected_contacts:
            return JSONResponse(
                status_code=400,
                content={"error": "No valid contacts selected for processing"}
            )
        
        # Process the selected contacts
        contact_count = len(selected_contacts)
        logger.debug("Processing %d selected contacts", contact_count)
        
        # Choose processing method based on number of contacts
        if contact_count < 100:
            results = main_sync(selected_contacts, start, end)
        else:
            results = await main_async(selected_contacts, start, end, batch_size=min(contact_count // 10, 1000))
        
        # Convert results to DataFrame for easier filtering and organization
        df_data = []
        for result in results:
            contact_id = result['contact_id']
            contact = next((c for c in selected_contacts if c['id'] == contact_id), None)
            if contact:
                # Add scheduled emails
                for email in result.get('scheduled', []):
                    df_data.append({
                        'contact_id': contact_id,
                        'first_name': contact.get('first_name', ''),
                        'last_name': contact.get('last_name', ''),
                        'email': contact.get('email', ''),
                        'state': contact.get('state', ''),
                        'birth_date': contact.get('birth_date'),
                        'effective_date': contact.get('effective_date'),
                        'email_type': email['type'],
                        'email_date': email['date'],
                        'skipped': 'No',
                        'reason': '',
                        'link': f"/contact/{contact_id}/email/{email['type']}/{email['date']}"
                    })
                
                # Add skipped emails
                for email in result.get('skipped', []):
                    df_data.append({
                        'contact_id': contact_id,
                        'first_name': contact.get('first_name', ''),
                        'last_name': contact.get('last_name', ''),
                        'email': contact.get('email', ''),
                        'state': contact.get('state', ''),
                        'birth_date': contact.get('birth_date'),
                        'effective_date': contact.get('effective_date'),
                        'email_type': email['type'],
                        'email_date': email.get('date', start),
                        'skipped': 'Yes',
                        'reason': email.get('reason', ''),
                        'link': ''
                    })

        # Create DataFrame and store in memory
        df = pd.DataFrame(df_data)
        org_data_store[org_id] = df
        
        # Prepare contact data for display
        contacts_data = {}
        for contact_id in contact_ids:
            contact_rows = df[df['contact_id'] == contact_id]
            if len(contact_rows) == 0:
                continue

            first_row = contact_rows.iloc[0]
            state_code = first_row['state']
            
            # Initialize contact data structure
            contacts_data[contact_id] = {
                'contact_info': {
                    'id': contact_id,
                    'name': f"{first_row['first_name']} {first_row['last_name']}",
                    'email': first_row['email'],
                    'state': state_code,
                    'birth_date': first_row['birth_date'],
                    'effective_date': first_row['effective_date']
                },
                'timeline_data': {
                    'email_list': []
                }
            }

            # Add applicable scheduling rules based on state
            rules = []
            if state_code in BIRTHDAY_RULE_STATES:
                window = BIRTHDAY_RULE_STATES[state_code]
                rules.append(f"Birthday emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after birthday")
            if state_code in EFFECTIVE_DATE_RULE_STATES:
                window = EFFECTIVE_DATE_RULE_STATES[state_code]
                rules.append(f"Effective date emails: {window.get('window_before', 0)} days before to {window.get('window_after', 0)} days after anniversary")
            if state_code in YEAR_ROUND_ENROLLMENT_STATES:
                rules.append("Year-round enrollment state - no scheduled emails")
            else:
                rules.append("AEP emails: Distributed across August/September")
                rules.append("Post-window emails: Day after exclusion period")
            contacts_data[contact_id]['scheduling_rules'] = rules

            # Add emails to timeline data
            email_list = []
            for _, row in contact_rows.iterrows():
                email_type = row['email_type']
                # Map email types to human-readable names and determine default dates
                email_info = {
                    'type': email_type,
                    'type_display': {
                        'birthday': 'Birthday Email',
                        'anniversary': 'Anniversary Email',
                        'aep': 'AEP Email',
                        'post_window': 'Post-Window Email'
                    }.get(email_type, email_type.replace('_', ' ').title()),
                    'start': row['email_date'],
                    'skipped': row['skipped'] == 'Yes',
                    'reason': row['reason'] if row['skipped'] == 'Yes' else '',
                    'link': row['link'],
                    'default_date': None  # Will be populated based on type
                }
                
                # Set default dates based on type
                if email_type == 'birthday' and first_row['birth_date']:
                    email_info['default_date'] = first_row['birth_date']
                elif email_type == 'anniversary' and first_row['effective_date']:
                    email_info['default_date'] = first_row['effective_date']
                elif email_type == 'aep':
                    email_info['default_date'] = 'AEP Window'
                elif email_type == 'post_window':
                    email_info['default_date'] = 'Post Exclusion Period'
                
                email_list.append(email_info)
            
            # Sort email list by date
            email_list.sort(key=lambda x: x['start'])
            contacts_data[contact_id]['timeline_data']['email_list'] = email_list
        
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "org_name": org['name'],
                "org_id": org_id,
                "contacts": contacts_data,
                "total_contacts": len(df.groupby('contact_id')),
                "sample_size": len(contacts_data),
                "sample_sizes": [5, 10, 25, 50, 100],
                "special_rule_states": SPECIAL_RULE_STATES,
                "current_date": start.isoformat(),
                "end_date": end.isoformat()
            }
        )
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error("Error in process_universe: %s\n%s", str(e), error_trace)
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error": f"Error processing contacts: {str(e)}\nTrace:\n{error_trace}"
            }
        )

@app.get("/universe", response_class=HTMLResponse)
async def universe_selection(request: Request):
    """Display the universe selection page"""
    # Default dates
    today = date.today()
    next_year = today + timedelta(days=365)
    
    return templates.TemplateResponse(
        "universe_selection.html",
        {
            "request": request,
            "title": "Universe Selection",
            "all_states": ALL_STATES,
            "special_rule_states": SPECIAL_RULE_STATES,
            "state_rules": {
                state: {
                    "has_birthday_rule": state in BIRTHDAY_RULE_STATES,
                    "has_effective_date_rule": state in EFFECTIVE_DATE_RULE_STATES,
                    "has_year_round_enrollment": state in YEAR_ROUND_ENROLLMENT_STATES
                }
                for state in ALL_STATES
            },
            "today": today.isoformat(),
            "next_year": next_year.isoformat()
        }
    )

@app.get("/preview_email")
async def preview_email(
    request: Request,
    org_id: int,
    contact_id: str,
    email_type: str,
    email_date: str
):
    """Preview an email template for a specific contact"""
    try:
        logger.debug(f"Previewing email for org_id={org_id}, contact_id={contact_id}, type={email_type}, date={email_date}")
        
        # Get contact directly from the org database
        org_db_path = os.path.join(org_db_dir, f'org-{org_id}.db')
        if not os.path.exists(org_db_path):
            logger.error(f"Organization database not found: {org_db_path}")
            raise HTTPException(status_code=404, detail="Organization database not found")
            
        contacts = get_contacts_from_org_db(org_db_path, org_id, contact_ids=[contact_id])
        if not contacts:
            logger.error(f"Contact {contact_id} not found in organization {org_id}")
            raise HTTPException(status_code=404, detail="Contact not found in organization")
            
        formatted_contacts = format_contact_data(contacts)
        if not formatted_contacts:
            logger.error(f"Failed to format contact data for contact {contact_id}")
            raise HTTPException(status_code=500, detail="Failed to format contact data")
            
        contact = formatted_contacts[0]
        logger.debug(f"Found contact: {contact}")
        
        # Get organization details from database
        try:
            async with aiosqlite.connect('main.db') as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    """SELECT id, name, phone, website, logo_url, primary_color, secondary_color, logo_data 
                       FROM organizations WHERE id = ?""", 
                    (org_id,)
                ) as cursor:
                    org_data = await cursor.fetchone()
                    if not org_data:
                        logger.error(f"Organization {org_id} not found in main database")
                        raise HTTPException(status_code=404, detail="Organization not found")
                    
                    # Create organization dict with correct keys for template
                    organization = dict(org_data)
                    logger.debug(f"Found organization details: {organization}")
        except Exception as e:
            logger.error(f"Error fetching organization details for org_id={org_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Error fetching organization details: {e}")
        
        # Parse email date
        try:
            parsed_email_date = datetime.strptime(email_date, "%Y-%m-%d").date()
            logger.debug(f"Parsed email date: {parsed_email_date}")
        except ValueError as e:
            logger.error(f"Invalid email date format: {email_date}")
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        
        # Initialize template engine
        template_engine = EmailTemplateEngine()
        
        # Generate quote link
        try:
            logger.info(f"Generating quote link with org_id={org_id}, contact_id={contact_id}, email_type={email_type}, email_date={email_date}")
            quote_link = generate_link(org_id, contact_id, email_type, email_date)
            logger.info(f"Generated quote link: {quote_link}")
        except Exception as e:
            logger.error(f"Error generating quote link: {e}")
            quote_link = f"#error-generating-link-{str(e)}"
        
        # Verify the link generation by checking if quote_link contains the expected pattern
        expected_pattern = f"{org_id}-{contact_id}-"
        if expected_pattern not in quote_link:
            logger.warning(f"Generated quote link doesn't contain expected pattern '{expected_pattern}': {quote_link}")
        
        # Create template_data dictionary with organization as a top-level key
        template_data = {**contact}
        template_data["organization"] = organization
        template_data["quote_link"] = quote_link
        template_data["email_date"] = parsed_email_date
        logger.info(f"Template data keys: {template_data.keys()}")
        logger.info(f"Quote link in template data: {template_data.get('quote_link')}")
        
        # Render HTML email
        try:
            html_content = template_engine.render_email(
                template_type=email_type,
                contact=template_data,
                email_date=parsed_email_date,
                html=True
            )
            logger.debug("Successfully rendered email template")
            return HTMLResponse(content=html_content['html'])  # Extract the 'html' key from the result
        except Exception as e:
            logger.error(f"Error rendering email template: {e}")
            raise HTTPException(status_code=500, detail=f"Error rendering template: {e}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error previewing email: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add the email sending page (GET) endpoint
@app.get("/send_emails", response_class=HTMLResponse)
async def send_emails_page(
    request: Request,
    org_id: int = Query(...),
    show_all: bool = Query(False),
    sample_size: int = Query(10),
    effective_date_filter: str = Query("none"),
    effective_date_years: int = Query(None),
    effective_date_start: str = Query(None),
    effective_date_end: str = Query(None),
    contact_search: str = Query(None),
    state: str = Query(None),
    special_rules_only: bool = Query(False),
    contact_ids: List[str] = Query(None)
):
    org_db_path = f"org_dbs/org-{org_id}.db"
    main_db_path = "main.db"
    org_details = get_organization_details(main_db_path, org_id)
    scheduler = EmailScheduler()
    
    formatted_contacts = []
    
    # If contact_ids are provided from the check page, use them directly
    if contact_ids:
        logger.info(f"Using {len(contact_ids)} specific contact IDs passed from check page")
        # Get the specific contacts by their IDs
        contacts = get_contacts_from_org_db(org_db_path, org_id, contact_ids=contact_ids)
        if contacts:
            formatted_contacts = format_contact_data(contacts)
            logger.info(f"Found {len(formatted_contacts)} contacts from check page by ID")

    # If no contacts found by ID, fall back to filtering
    if not formatted_contacts:
        logger.info("Contact IDs not provided or no matching contacts found, using filter parameters")
        
        # Convert effective date values to integers, handling -1 case
        effective_date_start_int = None
        effective_date_end_int = None
    
        if effective_date_start is not None and effective_date_start != "None":
            try:
                effective_date_start_int = int(effective_date_start)
            except ValueError:
                logger.error(f"Invalid effective_date_start value: {effective_date_start}")
                
        if effective_date_end is not None and effective_date_end != "None":
            try:
                effective_date_end_int = int(effective_date_end)
            except ValueError:
                logger.error(f"Invalid effective_date_end value: {effective_date_end}")
        
        # Calculate effective date range if filter is active
        effective_date_age_years = None
        effective_date_range_start = None
        effective_date_range_end = None
        
        if effective_date_filter == "single" and effective_date_years:
            effective_date_age_years = effective_date_years
        elif effective_date_filter == "range" and effective_date_start_int is not None:
            # Calculate date range based on first day of current month
            first_of_month = date.today().replace(day=1)
            
            # Handle start date (this will be the earlier/older date)
            if effective_date_start_int == -1:
                # No start limit
                effective_date_range_start = None
            else:
                # Calculate start date (earlier/older date)
                effective_date_range_start = first_of_month - timedelta(days=effective_date_start_int * 30)
                effective_date_range_start = effective_date_range_start.strftime("%Y-%m")
            
            # Handle end date (this will be the later/newer date)
            if effective_date_end_int is not None and effective_date_end_int != -1:
                # Calculate end date (later/newer date)
                effective_date_range_end = first_of_month - timedelta(days=effective_date_end_int * 30)
                effective_date_range_end = effective_date_range_end.strftime("%Y-%m")
            else:
                # No end limit
                effective_date_range_end = None
                
            # Swap start and end dates if needed (since larger months-ago number means earlier date)
            if (effective_date_range_start is not None and effective_date_range_end is not None and 
                effective_date_range_start < effective_date_range_end):
                effective_date_range_start, effective_date_range_end = effective_date_range_end, effective_date_range_start
        
        # Determine states to filter by
        states_to_filter = None
        if special_rules_only:
            states_to_filter = SPECIAL_RULE_STATES
            logger.debug(f"Filtering by special rules states: {states_to_filter}")
        elif state and state.strip():
            states_to_filter = [state]
            logger.debug(f"Filtering by specific state: {states_to_filter}")
        
        # Get filtered contacts based on criteria (same logic as check page)
        filtered_contacts = get_filtered_contacts_from_org_db(
            org_db_path, 
            org_id,
            states=states_to_filter,
            n=None,  # No limit when getting universe
            is_random=False,  # No randomization when getting universe
            effective_date_age_years=effective_date_age_years,
            effective_date_start=effective_date_range_start,
            effective_date_end=effective_date_range_end
        )
        
        # If searching for a specific contact, filter further
        if contact_search and contact_search.strip():
            search_term = contact_search.strip().lower()
            filtered_contacts = [
                contact for contact in filtered_contacts 
                if search_term in contact.get('email', '').lower() or 
                str(contact.get('id', '')) == search_term
            ]
        
        # Apply sample if not showing all
        if not show_all and not contact_search:
            if len(filtered_contacts) > sample_size:
                filtered_contacts = random.sample(filtered_contacts, sample_size)
        
        contacts = filtered_contacts
        formatted_contacts = format_contact_data(contacts)
    
    # Set date range
    today = date.today()
    end_date = today.replace(year=today.year + 1)

    # Generate email list - one email per contact from the filtered set
    emails = []
    today_str = today.isoformat()
    
    # Calculate date ranges for filtering
    today_date = today
    next_7_days = today_date + timedelta(days=7)
    next_30_days = today_date + timedelta(days=30)
    next_90_days = today_date + timedelta(days=90)
    next_year = today_date + timedelta(days=365)
    
    for contact in formatted_contacts:
        timeline = scheduler.process_contact(contact, today, end_date)
        contact_info = {
            "name": f"{contact['first_name']} {contact['last_name']}",
            "email": contact["email"]
        }
        
        # Get all scheduled emails (not skipped)
        scheduled = []
        for email in timeline.get("scheduled", []):
            email_date_str = email.get("scheduled_date") or email.get("date")
            
            # Parse the email date for date range categorization
            try:
                email_date = datetime.strptime(email_date_str, '%Y-%m-%d').date()
                
                # Determine date range category
                if email_date_str == today_str:
                    date_range = "today"
                elif email_date <= next_7_days:
                    date_range = "next_7_days"
                elif email_date <= next_30_days:
                    date_range = "next_30_days"
                elif email_date <= next_90_days:
                    date_range = "next_90_days"
                elif email_date <= next_year:
                    date_range = "next_year"
                else:
                    date_range = "future"
            except (ValueError, TypeError):
                # Default if we can't parse the date
                date_range = "unknown"
            
            scheduled.append({
                "contact": {"contact_info": contact_info},
                "type": email["type"],
                "type_display": {
                    "birthday": "Birthday Email",
                    "anniversary": "Effective Date Email",
                    "effective_date": "Effective Date Email",
                    "aep": "AEP Email",
                    "post_window": "Post-Window Email"
                }.get(email["type"], email["type"].replace("_", " ").title()),
                "start": email_date_str,
                "skipped": False,
                "is_today": email_date_str == today_str,
                "date_range": date_range
            })
        
        # Only include skipped emails for reference/completeness
        for email in timeline.get("skipped", []):
            email_date_str = email.get("scheduled_date") or email.get("date") or today_str
            
            # Parse the email date for date range categorization
            try:
                email_date = datetime.strptime(email_date_str, '%Y-%m-%d').date()
                
                # Determine date range category
                if email_date_str == today_str:
                    date_range = "today"
                elif email_date <= next_7_days:
                    date_range = "next_7_days"
                elif email_date <= next_30_days:
                    date_range = "next_30_days"
                elif email_date <= next_90_days:
                    date_range = "next_90_days"
                elif email_date <= next_year:
                    date_range = "next_year"
                else:
                    date_range = "future"
            except (ValueError, TypeError):
                # Default if we can't parse the date
                date_range = "unknown"
                
            scheduled.append({
                "contact": {"contact_info": contact_info},
                "type": email["type"],
                "type_display": {
                    "birthday": "Birthday Email",
                    "anniversary": "Effective Date Email",
                    "effective_date": "Effective Date Email",
                    "aep": "AEP Email",
                    "post_window": "Post-Window Email"
                }.get(email["type"], email["type"].replace("_", " ").title()),
                "start": email_date_str,
                "skipped": True,
                "is_today": email_date_str == today_str,
                "date_range": date_range
            })
            
        # Add all emails for this contact to the main list
        emails.extend(scheduled)

    return templates.TemplateResponse(
        "send_emails.html",
        {
            "request": request,
            "org_name": org_details["name"],
            "org_id": org_id,
            "emails": emails,
            "contacts": formatted_contacts,
            "show_all": show_all,
            "sample_size": sample_size,
            "effective_date_filter": effective_date_filter,
            "effective_date_years": effective_date_years,
            "effective_date_start": effective_date_start,
            "effective_date_end": effective_date_end,
            "contact_search": contact_search,
            "state": state,
            "special_rules_only": special_rules_only
        }
    )

# Add the email sending POST endpoint
@app.post("/send_emails")
async def send_emails(
    request: Request,
    org_id: int = Form(...),
    send_mode: str = Form(...),
    test_emails: str = Form(None),
    scope: str = Form(...),
    batch_size: int = Form(100),
    state: str = Form(None),
    special_rules_only: bool = Form(False),
    contact_ids: List[str] = Form([])
):
    """
    Send scheduled emails based on configuration.
    
    Args:
        org_id: Organization ID
        send_mode: 'test' or 'production' mode
        test_emails: Comma-separated list of test email addresses (required in test mode)
        scope: Which emails to send; one of:
               - 'bulk': One email per contact (highest priority email)
               - 'today': Only emails scheduled for today
               - 'next_7_days': Emails scheduled within the next 7 days
               - 'next_30_days': Emails scheduled within the next 30 days
               - 'next_90_days': Emails scheduled within the next 90 days
        batch_size: Maximum number of emails to send in this batch
        state: Optional state filter to apply
        special_rules_only: Whether to only include states with special rules
        contact_ids: List of specific contact IDs to process (overrides filtering parameters if provided)
    """
    # Validate inputs
    if send_mode not in ["test", "production"]:
        raise HTTPException(status_code=400, detail="Invalid send mode")
    if send_mode == "test" and (not test_emails or test_emails.strip() == ""):
        raise HTTPException(status_code=400, detail="Test emails required in test mode")
    if scope not in ["bulk", "today", "next_7_days", "next_30_days", "next_90_days"]:
        raise HTTPException(status_code=400, detail="Invalid scope")

    # Initialize components
    from sendgrid_client import SendGridClient
    
    # Determine if we should use dry run mode based on the send mode and environment variables
    use_dry_run = False
    if send_mode == "test":
        # In test mode, use dry run only if TEST_EMAIL_SENDING is disabled
        use_dry_run = not TEST_EMAIL_SENDING
        if not use_dry_run:
            logger.info(f"Test mode will send REAL emails to test addresses")
        else:
            logger.info(f"Test mode is in dry run - no emails will be sent")
    else:
        # In production mode, use dry run unless PRODUCTION_EMAIL_SENDING is enabled
        use_dry_run = not PRODUCTION_EMAIL_SENDING
        if not use_dry_run:
            logger.warning(f"PRODUCTION MODE - REAL EMAILS WILL BE SENT TO ACTUAL RECIPIENTS")
        else:
            logger.info(f"Production mode is in dry run - no emails will be sent")
    
    sendgrid_client = SendGridClient(dry_run=use_dry_run)
    template_engine = EmailTemplateEngine()
    scheduler = EmailScheduler()
    org_db_path = f"org_dbs/org-{org_id}.db"

    # Get organization details
    org_details = get_organization_details(main_db, org_id)
    
    # Initialize all variables with defaults to avoid UnboundLocalError
    form_data = await request.form()
    effective_date_filter = form_data.get("effective_date_filter", "none")
    effective_date_years = form_data.get("effective_date_years", None)
    effective_date_start = form_data.get("effective_date_start", None)
    effective_date_end = form_data.get("effective_date_end", None)
    contact_search = form_data.get("contact_search", None)
    show_all = form_data.get("show_all", "false").lower() == "true"
    sample_size = int(form_data.get("sample_size", "10"))
    
    # Initialize filtering variables
    effective_date_start_int = None
    effective_date_end_int = None
    effective_date_age_years = None
    effective_date_range_start = None
    effective_date_range_end = None
    contacts = []
    filtered_contacts = []

    # Get state filtering parameters from form if not provided directly
    if not state:  # Only use form state if not provided as parameter
        state = form_data.get("state", None)
    if not special_rules_only:  # Only use form special_rules if not provided as parameter
        special_rules_only = form_data.get("special_rules_only", "false").lower() == "true"
        
    # First check if we have the contact_ids passed from the send_emails form
    formatted_contacts = []
    
    if contact_ids:
        logger.info(f"Using {len(contact_ids)} specific contact IDs passed from send_emails form")
        # Get the specific contacts by their IDs
        contacts = get_contacts_from_org_db(org_db_path, org_id, contact_ids=contact_ids)
        if contacts:
            formatted_contacts = format_contact_data(contacts)
            logger.info(f"Found {len(formatted_contacts)} contacts from POST form by ID")
    
    # If no contacts found by ID, fall back to filtering parameters
    if not formatted_contacts:
        logger.info("No contact IDs provided or no matching contacts found, using filter parameters")
        
        # Convert effective date values if needed
        if effective_date_years and str(effective_date_years).strip():
            try:
                effective_date_years = int(effective_date_years)
            except ValueError:
                effective_date_years = None
        
        # Convert effective date values to integers, handling -1 case
        if effective_date_start and effective_date_start != "None":
            try:
                effective_date_start_int = int(effective_date_start)
            except ValueError:
                logger.error(f"Invalid effective_date_start value: {effective_date_start}")
                
        if effective_date_end and effective_date_end != "None":
            try:
                effective_date_end_int = int(effective_date_end)
            except ValueError:
                logger.error(f"Invalid effective_date_end value: {effective_date_end}")
    
    if effective_date_filter == "single" and effective_date_years:
        effective_date_age_years = effective_date_years
    elif effective_date_filter == "range" and effective_date_start_int is not None:
        # Calculate date range based on first day of current month
        first_of_month = date.today().replace(day=1)
        
        # Handle start date (this will be the earlier/older date)
        if effective_date_start_int == -1:
            # No start limit
            effective_date_range_start = None
        else:
            # Calculate start date (earlier/older date)
            effective_date_range_start = first_of_month - timedelta(days=effective_date_start_int * 30)
            effective_date_range_start = effective_date_range_start.strftime("%Y-%m")
        
        # Handle end date (this will be the later/newer date)
        if effective_date_end_int is not None and effective_date_end_int != -1:
            # Calculate end date (later/newer date)
            effective_date_range_end = first_of_month - timedelta(days=effective_date_end_int * 30)
            effective_date_range_end = effective_date_range_end.strftime("%Y-%m")
        else:
            # No end limit
            effective_date_range_end = None
            
        # Swap start and end dates if needed (since larger months-ago number means earlier date)
        if (effective_date_range_start is not None and effective_date_range_end is not None and 
            effective_date_range_start < effective_date_range_end):
            effective_date_range_start, effective_date_range_end = effective_date_range_end, effective_date_range_start
    
    # Determine states to filter by
    states_to_filter = None
    if special_rules_only:
        states_to_filter = SPECIAL_RULE_STATES
        logger.debug(f"Filtering by special rules states: {states_to_filter}")
    elif state and state.strip():
        states_to_filter = [state]
        logger.debug(f"Filtering by specific state: {states_to_filter}")
    
    # Get filtered contacts based on criteria (same logic as check page)
    filtered_contacts = get_filtered_contacts_from_org_db(
        org_db_path, 
        org_id,
        states=states_to_filter,
        n=None,  # No limit when getting universe
        is_random=False,  # No randomization when getting universe
        effective_date_age_years=effective_date_age_years,
        effective_date_start=effective_date_range_start,
        effective_date_end=effective_date_range_end
    )
    
    # If searching for a specific contact, filter further
    if contact_search and contact_search.strip():
        search_term = contact_search.strip().lower()
        filtered_contacts = [
            contact for contact in filtered_contacts 
            if search_term in contact.get('email', '').lower() or 
            str(contact.get('id', '')) == search_term
        ]
    
    # Apply sample if not showing all and not searching for specific contact
    if not show_all and not contact_search:
        if len(filtered_contacts) > sample_size:
            filtered_contacts = random.sample(filtered_contacts, sample_size)
    
    # If we don't have contacts from IDs, use the filtered contacts
    if not formatted_contacts:
        contacts = filtered_contacts
        formatted_contacts = format_contact_data(contacts)
    
    # Add organization information to each contact
    for contact in formatted_contacts:
        contact['organization'] = org_details
        if 'quote_link' not in contact:
            # Generate a link for the contact
            contact['quote_link'] = generate_link(org_id, contact['id'], 'effective_date', contact.get('effective_date'))

    # Set date range
    today = date.today()
    end_date = today.replace(year=today.year + 1)  # One year ahead

    # Get scheduled emails
    emails_to_send = []
    for contact in formatted_contacts:
        timeline = scheduler.process_contact(contact, today, end_date)
        scheduled_emails = timeline.get("scheduled", [])
        skipped_emails = timeline.get("skipped", [])

        # Calculate date ranges for filtering
        today_str = today.isoformat()
        next_7_days = today + timedelta(days=7)
        next_30_days = today + timedelta(days=30)
        next_90_days = today + timedelta(days=90)
        
        if scope == "bulk":
            # For bulk sends, always use post_window script since it's not specific to any time window
            # This is appropriate for both testing and production bulk sends
            
            # Create a post_window email for today
            logger.info(f"Creating post_window email for bulk send to contact {contact.get('id')}")
            emails_to_send.append({
                "contact": contact, 
                "type": "post_window", 
                "date": today_str
            })
            logger.info(f"Selected email type post_window for contact {contact.get('id')}")
                
        elif scope == "today":
            # Only today's emails
            emails_to_send.extend([
                {"contact": contact, "type": email["type"], "date": email.get("scheduled_date") or email.get("date")}
                for email in scheduled_emails
                if (email.get("scheduled_date") or email.get("date")) == today_str
            ])
            
        elif scope == "next_7_days":
            # Emails scheduled for the next 7 days
            emails_to_send.extend([
                {"contact": contact, "type": email["type"], "date": email.get("scheduled_date") or email.get("date")}
                for email in scheduled_emails
                if datetime.strptime(email.get("scheduled_date") or email.get("date"), '%Y-%m-%d').date() <= next_7_days
            ])
            
        elif scope == "next_30_days":
            # Emails scheduled for the next 30 days
            emails_to_send.extend([
                {"contact": contact, "type": email["type"], "date": email.get("scheduled_date") or email.get("date")}
                for email in scheduled_emails
                if datetime.strptime(email.get("scheduled_date") or email.get("date"), '%Y-%m-%d').date() <= next_30_days
            ])
            
        elif scope == "next_90_days":
            # Emails scheduled for the next 90 days
            emails_to_send.extend([
                {"contact": contact, "type": email["type"], "date": email.get("scheduled_date") or email.get("date")}
                for email in scheduled_emails
                if datetime.strptime(email.get("scheduled_date") or email.get("date"), '%Y-%m-%d').date() <= next_90_days
            ])

    # Apply batch size as limit if specified
    if batch_size and batch_size > 0:
        emails_to_send = emails_to_send[:batch_size]

    # If no emails to send, render email_table.html with error message
    if not emails_to_send:
        logger.warning(f"No emails to send for org_id={org_id}, scope={scope}")
        # Create string representation of all the filters for debugging
        filter_info = f"org_id={org_id}, scope={scope}, state={state}, special_rules_only={special_rules_only}, " \
                     f"effective_date_filter={effective_date_filter}, contacts={len(formatted_contacts)}"
        logger.warning(f"Filter info: {filter_info}")
        
        return templates.TemplateResponse(
            "email_table.html",
            {
                "request": request,
                "org_id": org_id,
                "org_name": org_details["name"],
                "message": "No emails to send based on current criteria",
                "total_sent": 0,
                "failures": 0,
                "contacts": formatted_contacts,
                "show_all": show_all,
                "sample_size": sample_size,
                "state": state,
                "special_rules_only": special_rules_only,
                "effective_date_filter": effective_date_filter,
                "effective_date_years": effective_date_years,
                "effective_date_start": effective_date_start,
                "effective_date_end": effective_date_end,
                "send_mode": send_mode
            }
        )

    # Prepare test email list
    test_email_list = []
    if send_mode == "test" and test_emails:
        # Split by commas, strip whitespace, and filter out empty strings
        test_email_list = [email.strip() for email in test_emails.split(",") if email.strip()]
    
    # Check if we have test emails in test mode
    if send_mode == "test" and not test_email_list:
        raise HTTPException(status_code=400, detail="No valid test email addresses provided")

    # Send emails in batches (using batch_size from function parameters)
    # Initialize counters
    total_sent = 0
    failures = 0
    
    for i in range(0, len(emails_to_send), batch_size):
        batch = emails_to_send[i:i + batch_size]
        for email in batch:
            contact = email["contact"]
            email_type = email["type"]
            email_date = email["date"]
            
            # Choose recipient based on mode
            recipient = random.choice(test_email_list) if send_mode == "test" else contact["email"]
            
            try:
                # Render email content
                content = template_engine.render_email(email_type, contact, email_date)
                html_content = template_engine.render_email(email_type, contact, email_date, html=True)

                # For test emails, use the same subject line as production
                # Do not add any test indicators to the subject line
                subject = content["subject"]
                
                # Send email with unmodified subject line
                success = sendgrid_client.send_email(
                    to_email=recipient,
                    subject=subject,
                    content=content["body"],
                    html_content=html_content["html"]
                )

                # Log to database
                log_email_send(org_db_path, contact["id"], email_type, email_date, send_mode, recipient, success)
                
                if success:
                    total_sent += 1
                else:
                    failures += 1
                    
            except Exception as e:
                logger.error(f"Error sending email to {recipient}: {e}")
                failures += 1
                # Log the error to database
                log_email_send(org_db_path, contact["id"], email_type, email_date, send_mode, recipient, False, str(e))

        # Delay between batches to respect rate limits
        if i + batch_size < len(emails_to_send):
            await asyncio.sleep(1)

    # Create a success message and show results
    dry_run_note = "[DRY RUN ONLY - NO ACTUAL EMAILS SENT] " if sendgrid_client.dry_run else ""
    message = f"{dry_run_note}Successfully processed {total_sent} emails in {send_mode} mode"
    if failures > 0:
        message += f" with {failures} failures"
    
    # Return a results page with the send details
    return templates.TemplateResponse(
        "email_table.html",
        {
            "request": request,
            "org_id": org_id,
            "org_name": org_details["name"],
            "message": message,
            "total_sent": total_sent,
            "failures": failures,
            "emails_sent": emails_to_send,
            "contacts": formatted_contacts,
            "show_all": show_all,
            "sample_size": sample_size,
            "state": state,
            "special_rules_only": special_rules_only,
            "effective_date_filter": effective_date_filter,
            "effective_date_years": effective_date_years,
            "effective_date_start": effective_date_start,
            "effective_date_end": effective_date_end,
            "send_mode": send_mode,
            "test_emails": test_emails if send_mode == "test" else None
        }
    )

def log_email_send(
    db_path: str, 
    contact_id: str, 
    email_type: str, 
    email_date: str, 
    mode: str, 
    recipient: str, 
    success: bool, 
    error_message: str = None
):
    """
    Log email send events to the contact_events table
    
    Args:
        db_path: Path to the organization's database
        contact_id: Contact ID
        email_type: Type of email (birthday, anniversary, aep, etc.)
        email_date: Scheduled date for the email
        mode: 'test' or 'production'
        recipient: Email address of the recipient
        success: Whether the send was successful
        error_message: Optional error message if send failed
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create contact_events table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contact_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER,
            lead_id INTEGER,
            event_type TEXT NOT NULL,
            metadata TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_id) REFERENCES contacts(id),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        )
        ''')
        
        # Prepare metadata as JSON
        metadata = {
            "email_type": email_type,
            "scheduled_date": email_date,
            "mode": mode,
            "recipient": recipient,
            "success": success
        }
        
        # Add error message if provided
        if error_message:
            metadata["error"] = error_message
            
        # Convert metadata to JSON string using CustomJSONEncoder to handle date objects
        metadata_json = json.dumps(metadata, cls=CustomJSONEncoder)
        
        # Insert the event
        cursor.execute(
            "INSERT INTO contact_events (contact_id, event_type, metadata) VALUES (?, ?, ?)",
            (contact_id, "email_sent", metadata_json)
        )
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error logging email send to database: {e}")
    finally:
        if conn:
            conn.close()

async def get_contact_by_id(contact_id: str) -> dict:
    """Get contact details by ID from the database"""
    # Convert contact_id to string for comparison
    contact_id_str = str(contact_id)
    
    # Since we store contacts in memory after processing, we can search through org_data_store
    for org_df in org_data_store.values():
        # Convert contact_id column to string for comparison
        contact_mask = org_df['contact_id'].astype(str) == contact_id_str
        if contact_mask.any():
            contact_data = org_df[contact_mask].iloc[0].to_dict()
            return {
                'id': str(contact_data['contact_id']),
                'first_name': contact_data['first_name'],
                'last_name': contact_data['last_name'],
                'email': contact_data['email'],
                'state': contact_data['state'],
                'birth_date': contact_data['birth_date'],
                'effective_date': contact_data['effective_date']
            }
    return None

if __name__ == "__main__":
    import uvicorn
    import argparse
    
    parser = argparse.ArgumentParser(description="Email Scheduler App")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")
    args = parser.parse_args()
    
    uvicorn.run(app, host="0.0.0.0", port=args.port) 