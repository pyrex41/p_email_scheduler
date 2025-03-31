from fastapi import FastAPI, Request, Form, Body
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
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

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
    get_contacts_from_org_db,
    format_contact_data,
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
                        'effective_date': 'Anniversary Email',
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
                elif email.get('type') == 'effective_date':
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
                        'effective_date': 'Anniversary Email',
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
                elif email.get('type') == 'effective_date':
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

@app.post("/check", response_class=HTMLResponse)
async def check_schedules(
    request: Request,
    org_id: int = Form(...),
    sample_size: int = Form(10),
    state: Optional[str] = Form(default=None),
    special_rules_only: bool = Form(default=False),
    contact_search: Optional[str] = Form(default=None)
):
    """Process organization's contacts and display sample results"""
    try:
        # Set date range
        current_date = date.today()
        end_date = date(current_date.year + 2, current_date.month, current_date.day)

        await refresh_databases(org_id)
        
        # Get organization details and contacts
        org = get_organization_details(main_db, org_id)
        org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
        contacts = get_contacts_from_org_db(org_db_path, org_id)
        formatted_contacts = format_contact_data(contacts)

        if not formatted_contacts:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": "No valid contacts found for scheduling"
                }
            )

        # Ensure dates are in ISO string format for the scheduler
        for contact in formatted_contacts:
            if contact.get('birth_date') and not isinstance(contact['birth_date'], str):
                contact['birth_date'] = contact['birth_date'].isoformat()
            if contact.get('effective_date') and not isinstance(contact['effective_date'], str):
                contact['effective_date'] = contact['effective_date'].isoformat()

        # Process contacts using the simplified async approach
        try:
            sample_ids = random.sample(range(len(formatted_contacts)), sample_size)
            sampled_contacts = [formatted_contacts[i] for i in sample_ids]
            if sample_size < 100:
                results = main_sync(sampled_contacts, current_date, end_date)
            elif sample_size < 1000:
                results = await main_async(sampled_contacts, current_date, end_date, batch_size=sample_size // 10)
            else:
                results = await main_async(sampled_contacts, current_date, end_date, batch_size=1000)
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to process contacts: {str(e)}\nTrace:\n{error_trace}"
                }
            )

        # Convert results to DataFrame for easier filtering and sampling
        df_data = []
        for result in results:
            contact_id = result['contact_id']
            contact = next((c for c in formatted_contacts if c['id'] == contact_id), None)
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

        # Apply filtering and sampling
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
        else:
            filtered_df = df.copy()
            if special_rules_only:
                filtered_df = filtered_df[filtered_df['state'].isin(SPECIAL_RULE_STATES)]
            elif state and state.strip():
                filtered_df = filtered_df[filtered_df['state'] == state]

        # Sample contacts
        unique_contacts = filtered_df.groupby('contact_id').first().reset_index()
        if len(unique_contacts) == 0:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": "No contacts found matching the filter criteria."
                }
            )

        sample_ids = sample_contacts_from_states(unique_contacts, sample_size, state if state and state.strip() else None)
        filtered_df = filtered_df[filtered_df['contact_id'].isin(sample_ids)]

        # Prepare contact data for display
        contacts_data = {}
        for contact_id in sample_ids:
            contact_rows = filtered_df[filtered_df['contact_id'] == contact_id]
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
                        'effective_date': 'Anniversary Email',
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
                elif email_type == 'effective_date' and first_row['effective_date']:
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
                "current_date": current_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        )
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error": f"Unexpected error: {str(e)}\nTrace:\n{error_trace}"
            }
        )

if __name__ == "__main__":
    import uvicorn
    import argparse
    
    parser = argparse.ArgumentParser(description="Email Scheduler App")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")
    args = parser.parse_args()
    
    uvicorn.run(app, host="0.0.0.0", port=args.port) 