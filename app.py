from fastapi import FastAPI, Request, Form, Body, Query
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi import HTTPException
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
from email_batch_manager import EmailBatchManager
import aiosqlite
import sqlite3
import uuid
from dotenv_config import load_env, get_email_config
import time

# Load environment variables from .env file
load_env()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Get email configuration from environment
email_config = get_email_config()
TEST_EMAIL_SENDING = email_config["test_email_sending"]
PRODUCTION_EMAIL_SENDING = email_config["production_email_sending"]

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

# Initialize the Email Batch Manager
batch_manager = EmailBatchManager()

@app.get("/email_batch/toggle_test_email")
async def toggle_test_email(request: Request):
    """Toggle the test email field based on send mode."""
    try:
        # Get the send mode from query params
        query_params = dict(request.query_params)
        send_mode = query_params.get("send_mode", "test")
        
        # Render the appropriate partial
        return templates.TemplateResponse(
            "partials/test_email_section.html", 
            {
                "request": request,
                "send_mode": send_mode
            }
        )
    except Exception as e:
        logger.error(f"Error toggling test email section: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/email_batch/new_batch_form")
async def email_batch_new_form(
    request: Request, 
    org_id: int = Query(...),
    contact_ids: Optional[str] = Query(None)
):
    """Render the form for creating a new email batch."""
    try:
        # Check if this is an HTMX request
        is_htmx = request.headers.get("HX-Request") == "true"
        logger.info(f"Received request to new_batch_form endpoint (HTMX: {is_htmx})")
        
        # Process contact IDs if provided
        contact_id_list = []
        if contact_ids:
            contact_id_list = [id.strip() for id in contact_ids.split(',') if id.strip()]
        
        # For organization-level batch, get all contacts if none provided
        if not contact_id_list:
            # Get all contacts for this organization
            all_contacts = get_all_contact_ids(org_id)
            contact_id_list = [str(c['id']) for c in all_contacts]
        
        # Get scheduled email counts for different date ranges and email types
        today = date.today()
        
        # Initialize counts
        counts = {
            "today_count": 0,
            "next_7_count": 0,
            "next_30_count": 0,
            "total_count": 0,
            "birthday_count": 0,
            "effective_date_count": 0,
            "aep_count": 0,
            "post_window_count": len(contact_id_list)  # Default for bulk mode
        }
        
        has_contacts_no_emails = True
        
        # Get scheduled emails JSON for these contacts
        try:
            schedule_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_dir/scheduled_emails.json")
            with open(schedule_file, 'r') as f:
                all_scheduled = json.load(f)
                
            # Filter by contact IDs
            scheduled_emails = [c for c in all_scheduled if str(c.get('contact_id')) in contact_id_list]
            
            # Count scheduled emails by date range and type
            total_emails_scheduled = 0
            for contact in scheduled_emails:
                emails = contact.get('emails', [])
                if emails:
                    has_contacts_no_emails = False
                    
                for email in emails:
                    # Skip skipped emails
                    if email.get('skipped', False):
                        continue
                        
                    total_emails_scheduled += 1
                    email_type = email.get('type')
                    email_date_str = email.get('date')
                    
                    try:
                        email_date = datetime.strptime(email_date_str, "%Y-%m-%d").date()
                        
                        # Count by date
                        if email_date == today:
                            counts["today_count"] += 1
                        
                        if today <= email_date <= today + timedelta(days=7):
                            counts["next_7_count"] += 1
                            
                        if today <= email_date <= today + timedelta(days=30):
                            counts["next_30_count"] += 1
                            
                        counts["total_count"] += 1
                        
                        # Count by type
                        if email_type == "birthday":
                            counts["birthday_count"] += 1
                        elif email_type == "effective_date":
                            counts["effective_date_count"] += 1
                        elif email_type == "aep":
                            counts["aep_count"] += 1
                        elif email_type == "post_window":
                            counts["post_window_count"] += 1
                    except Exception as e:
                        # Skip invalid dates
                        logger.warning(f"Invalid date format in scheduled email: {email_date_str} - {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error loading scheduled emails for batch page: {e}")
            # Continue anyway, we'll just have zero counts
        
        # Get organization details
        org = get_organization_details(org_id)
        
        # For HTMX requests, return just the form
        if is_htmx:
            return templates.TemplateResponse(
                "partials/batch_config_form.html", 
                {
                    "request": request,
                    "org_id": org_id,
                    "org_name": org['name'] if org else f"Organization {org_id}",
                    "contact_ids": contact_id_list,
                    "contact_count": len(contact_id_list),
                    "has_contacts_no_emails": has_contacts_no_emails,
                    "today": today.isoformat(),
                    **counts
                }
            )
        
        # For regular requests, redirect to the email batch page
        contact_ids_query = "&".join([f"contact_ids={cid}" for cid in contact_id_list])
        return RedirectResponse(
            url=f"/email_batch?org_id={org_id}&{contact_ids_query}",
            status_code=303
        )
        
    except Exception as e:
        logger.error(f"Error preparing new batch form: {e}")
        if is_htmx:
            return templates.TemplateResponse(
                "partials/error_message.html", 
                {
                    "request": request,
                    "error": str(e)
                }
            )
        raise HTTPException(status_code=500, detail=str(e))

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

# Batch operation models
class BatchInitParams(BaseModel):
    org_id: int
    contact_ids: List[str]
    email_types: List[str] 
    send_mode: str
    test_email: Optional[str] = None
    scope: str = "all"

class BatchProcessParams(BaseModel):
    batch_id: str
    chunk_size: int = 25

class BatchResumeParams(BaseModel):
    batch_id: str
    chunk_size: int = 25

class BatchRetryParams(BaseModel):
    batch_id: str
    chunk_size: int = 25

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
    # Start timing for performance tracking
    start_time = time.time()
    
    # Validate inputs
    if send_mode not in ["test", "production"]:
        raise HTTPException(status_code=400, detail="Invalid send mode")
    if send_mode == "test" and (not test_emails or test_emails.strip() == ""):
        raise HTTPException(status_code=400, detail="Test emails required in test mode")
    if scope not in ["bulk", "today", "next_7_days", "next_30_days", "next_90_days"]:
        raise HTTPException(status_code=400, detail="Invalid scope")

    # Initialize components
    from sendgrid_client import SendGridClient
    
    # Generate a batch ID for tracking
    batch_id = f"batch_{uuid.uuid4().hex[:10]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Starting email batch {batch_id} for org_id={org_id}, scope={scope}")
    
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

    # Create the email_send_tracking table if it doesn't exist
    try:
        conn = sqlite3.connect(org_db_path)
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations/add_email_tracking.sql"), 'r') as f:
            migration_sql = f.read()
            conn.executescript(migration_sql)
        conn.close()
    except Exception as e:
        logger.error(f"Error ensuring email_send_tracking table exists: {e}")
        # Continue as the table might already exist

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
        
    # Check if we should use all contacts instead of just the displayed ones
    query_params = dict(request.query_params)
    send_to_all = query_params.get("send_to_all", "false").lower() == "true"
    
    formatted_contacts = []
    
    if send_to_all:
        # Skip using the contact_ids and use all contacts matching the filters instead
        logger.info("Using ALL contacts matching filters, ignoring contact_ids")
        # The contact processing will happen in the "if not formatted_contacts" block below
    elif contact_ids:
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
    
    # Set a flag to indicate if we're using all contacts or just the displayed ones
    using_all_contacts = send_to_all
    
    # Connect to database for email tracking
    conn = sqlite3.connect(org_db_path)
    cursor = conn.cursor()
    
    try:
        # First, check which emails have already been sent to avoid duplicates
        # This is the main improvement to the send_emails function - track what's been sent
        sent_emails = {}
        cursor.execute("""
            SELECT contact_id, email_type, scheduled_date 
            FROM email_send_tracking 
            WHERE org_id = ? AND send_status = 'sent'
        """, (org_id,))
        
        for row in cursor.fetchall():
            contact_id, email_type, scheduled_date = row
            key = f"{contact_id}_{email_type}_{scheduled_date}"
            sent_emails[key] = True
            
        logger.info(f"Found {len(sent_emails)} previously sent emails for org_id={org_id}")
        
        # Process contacts to find unsent emails
        for contact in formatted_contacts:
            timeline = scheduler.process_contact(contact, today, end_date)
            scheduled_emails = timeline.get("scheduled", [])

            # Calculate date ranges for filtering
            today_str = today.isoformat()
            next_7_days = today + timedelta(days=7)
            next_30_days = today + timedelta(days=30)
            next_90_days = today + timedelta(days=90)
            
            if scope == "bulk":
                # For bulk sends, always use post_window script since it's not specific to any time window
                # This is appropriate for both testing and production bulk sends
                
                # Check if this bulk email has already been sent
                contact_id = str(contact.get('id'))
                email_type = "post_window"
                key = f"{contact_id}_{email_type}_{today_str}"
                
                if key not in sent_emails:
                    logger.info(f"Creating post_window email for bulk send to contact {contact_id}")
                    emails_to_send.append({
                        "contact": contact, 
                        "type": email_type, 
                        "date": today_str
                    })
                else:
                    logger.info(f"Skipping already sent post_window email for contact {contact_id}")
                    
            elif scope == "today":
                # Only today's emails
                for email in scheduled_emails:
                    email_date = email.get("scheduled_date") or email.get("date")
                    if email_date == today_str:
                        contact_id = str(contact.get('id'))
                        email_type = email["type"]
                        key = f"{contact_id}_{email_type}_{email_date}"
                        
                        if key not in sent_emails:
                            emails_to_send.append({
                                "contact": contact, 
                                "type": email_type, 
                                "date": email_date
                            })
                
            elif scope == "next_7_days":
                # Emails scheduled for the next 7 days
                for email in scheduled_emails:
                    email_date = email.get("scheduled_date") or email.get("date")
                    if datetime.strptime(email_date, '%Y-%m-%d').date() <= next_7_days:
                        contact_id = str(contact.get('id'))
                        email_type = email["type"]
                        key = f"{contact_id}_{email_type}_{email_date}"
                        
                        if key not in sent_emails:
                            emails_to_send.append({
                                "contact": contact, 
                                "type": email_type, 
                                "date": email_date
                            })
                
            elif scope == "next_30_days":
                # Emails scheduled for the next 30 days
                for email in scheduled_emails:
                    email_date = email.get("scheduled_date") or email.get("date")
                    if datetime.strptime(email_date, '%Y-%m-%d').date() <= next_30_days:
                        contact_id = str(contact.get('id'))
                        email_type = email["type"]
                        key = f"{contact_id}_{email_type}_{email_date}"
                        
                        if key not in sent_emails:
                            emails_to_send.append({
                                "contact": contact, 
                                "type": email_type, 
                                "date": email_date
                            })
                
            elif scope == "next_90_days":
                # Emails scheduled for the next 90 days
                for email in scheduled_emails:
                    email_date = email.get("scheduled_date") or email.get("date")
                    if datetime.strptime(email_date, '%Y-%m-%d').date() <= next_90_days:
                        contact_id = str(contact.get('id'))
                        email_type = email["type"]
                        key = f"{contact_id}_{email_type}_{email_date}"
                        
                        if key not in sent_emails:
                            emails_to_send.append({
                                "contact": contact, 
                                "type": email_type, 
                                "date": email_date
                            })
        
        # Log the eligible unsent emails
        logger.info(f"Found {len(emails_to_send)} unsent emails matching criteria for org_id={org_id}")
        
        # Register all emails in the tracking table before sending
        for email in emails_to_send:
            contact = email["contact"]
            email_type = email["type"]
            email_date = email["date"]
            contact_id = str(contact.get('id'))
            
            # First, check if this exact email is already registered
            cursor.execute("""
                SELECT id, send_status FROM email_send_tracking
                WHERE org_id = ? AND contact_id = ? AND email_type = ? AND scheduled_date = ? AND batch_id = ?
            """, (org_id, contact_id, email_type, email_date, batch_id))
            
            existing = cursor.fetchone()
            
            if not existing:
                # Register the email in the tracking table with 'pending' status
                cursor.execute("""
                    INSERT INTO email_send_tracking
                    (org_id, contact_id, email_type, scheduled_date, send_status, send_mode, test_email, batch_id)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                """, (
                    org_id,
                    contact_id,
                    email_type,
                    email_date,
                    send_mode,
                    test_emails if send_mode == 'test' else None,
                    batch_id
                ))
        
        # Commit the registrations
        conn.commit()
        
        # Apply batch size as limit if specified
        if batch_size and batch_size > 0 and len(emails_to_send) > batch_size:
            logger.info(f"Limiting batch to {batch_size} emails out of {len(emails_to_send)} total")
            emails_to_send = emails_to_send[:batch_size]

        # If no emails to send, render email_table.html with error message
        if not emails_to_send:
            logger.warning(f"No new emails to send for org_id={org_id}, scope={scope}")
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
                    "message": "No new emails to send based on current criteria",
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
                    "send_mode": send_mode,
                    "batch_id": batch_id
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

        # Send emails in batches using the batch_size parameter
        # Initialize counters
        total_sent = 0
        failures = 0
        
        # Use semaphore to limit concurrent operations, similar to the optimized batch code
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent sends
        
        # Define an async function to process a single email
        async def process_email(email_data):
            async with semaphore:  # Ensure we don't overwhelm the system
                contact = email_data["contact"]
                email_type = email_data["type"]
                email_date = email_data["date"]
                contact_id = str(contact.get('id'))
                
                # Choose recipient based on mode
                recipient = random.choice(test_email_list) if send_mode == 'test' else contact["email"]
                
                try:
                    # Render email content
                    content = await asyncio.to_thread(
                        template_engine.render_email,
                        email_type,
                        contact,
                        email_date
                    )
                    
                    html_content = await asyncio.to_thread(
                        template_engine.render_email,
                        email_type,
                        contact,
                        email_date,
                        html=True
                    )

                    # Set subject line (add [TEST] prefix for test mode)
                    subject = content["subject"]
                    if send_mode == 'test':
                        subject = f"[TEST] {subject}"
                    
                    # Send email - use thread to avoid blocking
                    logger.info(f"Sending {email_type} email to contact {contact_id} via {recipient}")
                    try:
                        success = await asyncio.to_thread(
                            sendgrid_client.send_email,
                            to_email=recipient,
                            subject=subject,
                            content=content["body"],
                            html_content=html_content["html"]
                        )
                        if not success:
                            logger.error(f"SendGrid returned failure for {email_type} email to contact {contact_id} via {recipient}")
                    except Exception as send_err:
                        logger.error(f"Exception during SendGrid call for contact {contact_id}: {str(send_err)}")
                        raise send_err

                    # Update tracking record with result
                    update_time = datetime.now().isoformat()
                    if success:
                        cursor.execute("""
                            UPDATE email_send_tracking
                            SET send_status = 'sent',
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?
                            WHERE org_id = ? AND contact_id = ? AND email_type = ? AND scheduled_date = ? AND batch_id = ?
                        """, (
                            update_time,
                            org_id,
                            contact_id,
                            email_type,
                            email_date,
                            batch_id
                        ))
                        return {"success": True, "contact_id": contact_id}
                    else:
                        cursor.execute("""
                            UPDATE email_send_tracking
                            SET send_status = 'failed',
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?,
                                last_error = ?
                            WHERE org_id = ? AND contact_id = ? AND email_type = ? AND scheduled_date = ? AND batch_id = ?
                        """, (
                            update_time,
                            "Failed to send email",
                            org_id,
                            contact_id,
                            email_type,
                            email_date,
                            batch_id
                        ))
                        return {"success": False, "contact_id": contact_id, "error": "Failed to send email"}
                        
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Error sending {email_type} email to contact {contact_id}: {error_message}")
                    
                    # Update tracking record with error
                    update_time = datetime.now().isoformat()
                    cursor.execute("""
                        UPDATE email_send_tracking
                        SET send_status = 'failed',
                            send_attempt_count = send_attempt_count + 1,
                            last_attempt_date = ?,
                            last_error = ?
                        WHERE org_id = ? AND contact_id = ? AND email_type = ? AND scheduled_date = ? AND batch_id = ?
                    """, (
                        update_time,
                        error_message[:500],
                        org_id,
                        contact_id,
                        email_type,
                        email_date,
                        batch_id
                    ))
                    return {"success": False, "contact_id": contact_id, "error": error_message[:100]}
        
        # Process all emails in parallel
        tasks = [process_email(email) for email in emails_to_send]
        results = await asyncio.gather(*tasks)
        conn.commit()
        
        # Count results
        for result in results:
            if result["success"]:
                total_sent += 1
            else:
                failures += 1
        
        # Calculate performance metrics
        end_time = time.time()
        duration = end_time - start_time
        emails_per_second = len(emails_to_send) / duration if duration > 0 else 0
        
        logger.info(
            f"Batch {batch_id} completed in {duration:.2f}s: "
            f"{total_sent} sent, {failures} failed, "
            f"{emails_per_second:.1f} emails/second"
        )
        
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
                "test_emails": test_emails if send_mode == "test" else None,
                "using_all_contacts": using_all_contacts,
                "batch_id": batch_id,
                "processing_time": f"{duration:.2f}s",
                "emails_per_second": f"{emails_per_second:.1f}"
            }
        )
    
    except Exception as e:
        logger.error(f"Error in send_emails endpoint: {e}")
        # Rollback any pending database changes
        conn.rollback()
        
        # Return error page
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "org_id": org_id,
                "message": f"Error processing email batch: {str(e)}",
                "details": traceback.format_exc()
            }
        )
    
    finally:
        # Ensure connection is closed
        conn.close()

def log_email_send(
    db_path: str, 
    contact_id: str, 
    email_type: str, 
    email_date: str, 
    mode: str, 
    recipient: str, 
    success: bool, 
    error_message: str = None,
    batch_id: str = None
):
    """
    Log email send events to both contact_events and email_send_tracking tables
    
    Args:
        db_path: Path to the organization's database
        contact_id: Contact ID
        email_type: Type of email (birthday, anniversary, aep, etc.)
        email_date: Scheduled date for the email
        mode: 'test' or 'production'
        recipient: Email address of the recipient
        success: Whether the send was successful
        error_message: Optional error message if send failed
        batch_id: Optional batch ID for tracking (auto-generated if not provided)
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get current timestamp in ISO format
        timestamp = datetime.now().isoformat()
        
        # Get organization ID from the database path
        try:
            org_id = int(db_path.split('org-')[1].split('.')[0])
        except:
            org_id = 0
            logger.warning(f"Could not extract org_id from db_path: {db_path}, using 0")
        
        # Generate a batch ID if not provided
        if not batch_id:
            batch_id = f"auto_{uuid.uuid4().hex[:8]}_{timestamp.replace(':', '').replace('-', '').replace('.', '')}"
        
        # 1. Create and update email_send_tracking record
        # First, ensure the email_send_tracking table exists
        try:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations/add_email_tracking.sql"), 'r') as f:
                migration_sql = f.read()
                conn.executescript(migration_sql)
        except Exception as e:
            logger.error(f"Error ensuring email_send_tracking table exists: {e}")
            # Continue as the table might already exist
        
        # Check if an entry already exists for this email
        cursor.execute("""
            SELECT id FROM email_send_tracking
            WHERE org_id = ? AND contact_id = ? AND email_type = ? AND scheduled_date = ?
            ORDER BY created_at DESC LIMIT 1
        """, (org_id, contact_id, email_type, email_date))
        
        existing_record = cursor.fetchone()
        
        if existing_record:
            # Update existing record
            record_id = existing_record[0]
            status = "sent" if success else "failed"
            
            cursor.execute("""
                UPDATE email_send_tracking
                SET send_status = ?,
                    send_attempt_count = send_attempt_count + 1,
                    last_attempt_date = ?,
                    last_error = ?
                WHERE id = ?
            """, (status, timestamp, error_message if not success else None, record_id))
            
            logger.debug(f"Updated email_send_tracking record id={record_id} with status={status}")
        else:
            # Create a new record
            status = "sent" if success else "failed"
            test_email = recipient if mode == "test" else None
            
            cursor.execute("""
                INSERT INTO email_send_tracking (
                    org_id, contact_id, email_type, scheduled_date,
                    send_status, send_mode, test_email, send_attempt_count,
                    last_attempt_date, last_error, batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                org_id, contact_id, email_type, email_date,
                status, mode, test_email, 1,
                timestamp, error_message if not success else None, batch_id
            ))
            
            logger.debug(f"Created new email_send_tracking record for contact_id={contact_id}, type={email_type}")
        
        # 2. Also log to traditional contact_events table for backward compatibility
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
            "success": success,
            "batch_id": batch_id
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
        
        # Commit all changes
        conn.commit()
        logger.info(f"Logged email {email_type} for contact {contact_id}, success={success}")
        
    except Exception as e:
        logger.error(f"Error logging email send to database: {e}")
        # Try to rollback in case of error
        try:
            if conn:
                conn.rollback()
        except:
            pass
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
    
# Batch Email Management API Endpoints

class BatchInitParams(BaseModel):
    org_id: int
    contact_ids: List[str]
    email_types: List[str]
    send_mode: str
    test_email: Optional[str] = None
    scope: str = "all"
    chunk_size: Optional[int] = 25

class BatchProcessParams(BaseModel):
    batch_id: str
    chunk_size: Optional[int] = 25

class BatchRetryParams(BaseModel):
    batch_id: str
    chunk_size: Optional[int] = 100

class BatchResumeParams(BaseModel):
    batch_id: str
    chunk_size: Optional[int] = 100

@app.get("/email_batch", response_class=HTMLResponse)
async def email_batch_page(
    request: Request,
    org_id: int = Query(...),
    contact_ids: List[str] = Query([])
):
    """Show the batch email management page."""
    try:
        # Get organization details
        org_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "org_dbs", f"org-{org_id}.db")
        main_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.db")
        org_details = get_organization_details(main_db_path, org_id)
        
        # Load scheduled emails from JSON files
        schedule_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_dir")
        schedule_file = os.path.join(schedule_directory, "scheduled_emails.json")
        
        if not os.path.exists(schedule_file):
            raise HTTPException(status_code=404, detail=f"Scheduled emails file not found: {schedule_file}")
        
        try:
            with open(schedule_file, 'r') as f:
                scheduled_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading scheduled emails from {schedule_file}: {e}")
            raise HTTPException(status_code=500, detail=f"Error loading scheduled emails: {e}")
        
        # Calculate date ranges for scope options
        today = date.today()
        next_7_days = today + timedelta(days=7)
        next_30_days = today + timedelta(days=30)
        
        # Filter emails by contact_ids if provided
        emails = []
        
        # Keep track of which contact IDs actually have data
        included_contact_ids = set()
        
        for contact_data in scheduled_data:
            contact_id = contact_data.get('contact_id')
            included_contact_ids.add(str(contact_id))
            
            # Skip if contact_id not in the list (if filtering is applied)
            if contact_ids and str(contact_id) not in contact_ids:
                continue
            
            # Include all emails for this contact
            emails.extend(contact_data.get('emails', []))
        
        # Count emails by type and date range
        today_count = 0
        next_7_count = 0
        next_30_count = 0
        total_count = 0
        
        birthday_count = 0
        effective_date_count = 0
        aep_count = 0
        post_window_count = 0
        
        for email in emails:
            if email.get('skipped', False):
                continue
                
            total_count += 1
            email_type = email.get('type')
            
            if email_type == 'birthday':
                birthday_count += 1
            elif email_type in ['effective_date', 'anniversary']:
                effective_date_count += 1
            elif email_type == 'aep':
                aep_count += 1
            elif email_type == 'post_window':
                post_window_count += 1
            
            try:
                email_date = datetime.strptime(email.get('date', '2099-01-01'), "%Y-%m-%d").date()
                
                if email_date == today:
                    today_count += 1
                    next_7_count += 1
                    next_30_count += 1
                elif email_date <= next_7_days:
                    next_7_count += 1
                    next_30_count += 1
                elif email_date <= next_30_days:
                    next_30_count += 1
            except:
                continue
        
        # If we have contacts but no scheduled emails, set a flag to indicate this
        # This will help explain to the user why the counts are zero
        has_contacts_no_emails = len(contact_ids) > 0 and total_count == 0
        
        return templates.TemplateResponse(
            "email_batch.html",
            {
                "request": request,
                "org_name": org_details["name"],
                "org_id": org_id,
                "contact_ids": contact_ids,
                "today_count": today_count,
                "next_7_count": next_7_count,
                "next_30_count": next_30_count,
                "total_count": total_count,
                "birthday_count": birthday_count,
                "effective_date_count": effective_date_count,
                "aep_count": aep_count,
                "post_window_count": post_window_count,
                "has_contacts_no_emails": has_contacts_no_emails
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error preparing batch email page: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Initialize batch endpoint handling both multipart/form-data and JSON
@app.post("/api/initialize_batch")
async def initialize_batch_endpoint(request: Request):
    """Initialize a new email batch using form data or JSON."""
    try:
        # Check if this is an HTMX request
        is_htmx = request.headers.get("HX-Request") == "true"
        logger.info(f"Received request to initialize_batch endpoint (HTMX: {is_htmx})")
        
        # Log the raw request details for debugging
        content_type = request.headers.get("content-type", "").lower()
        logger.info(f"Request content type: {content_type}")
        
        # Different parsing based on content type
        if "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
            # Parse as form data
            form = await request.form()
            logger.info("Parsed request as form data")

            # Get required fields with defaults
            try:
                org_id = int(form.get("org_id", "0"))
                scope = form.get("scope", "all")
                send_mode = form.get("send_mode", "test")
                test_email = form.get("test_email", None)
                chunk_size = int(form.get("chunk_size", "25"))
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid form data: {str(e)}")
            
            # Extract multi-value fields 
            contact_ids = []
            email_types = []
            
            # Get all values from the form (both single and multi-valued)
            for key, value in form.items():
                if key == 'contact_ids':
                    contact_ids.append(str(value))
                elif key == 'email_types':
                    email_types.append(str(value))
            
            # Also process multi-items (for multiple values with same key)
            multi_items = list(form.multi_items())
            for key, value in multi_items:
                if key == 'contact_ids' and str(value) not in contact_ids:
                    contact_ids.append(str(value))
                elif key == 'email_types' and str(value) not in email_types:
                    email_types.append(str(value))
                    
        elif "application/json" in content_type:
            # Parse as JSON
            json_data = await request.json()
            logger.info("Parsed request as JSON data")
            
            # Extract fields from JSON
            org_id = json_data.get("org_id", 0)
            scope = json_data.get("scope", "all")
            send_mode = json_data.get("send_mode", "test")
            test_email = json_data.get("test_email")
            contact_ids = json_data.get("contact_ids", [])
            email_types = json_data.get("email_types", [])
            chunk_size = json_data.get("chunk_size", 25)
            
        else:
            # Try to guess the format from the raw body
            body = await request.body()
            logger.info(f"Unknown content type, trying to parse raw body (length: {len(body)})")
            
            try:
                # Try to parse as JSON first
                body_text = body.decode('utf-8')
                if body_text.strip().startswith('{'):
                    json_data = json.loads(body_text)
                    logger.info("Parsed raw body as JSON")
                    
                    # Extract fields from JSON
                    org_id = json_data.get("org_id", 0)
                    scope = json_data.get("scope", "all")
                    send_mode = json_data.get("send_mode", "test")
                    test_email = json_data.get("test_email")
                    contact_ids = json_data.get("contact_ids", [])
                    email_types = json_data.get("email_types", [])
                    chunk_size = json_data.get("chunk_size", 25)
                else:
                    # Try form URL-encoded
                    import urllib.parse
                    form_data = urllib.parse.parse_qs(body_text)
                    logger.info(f"Parsed raw body as form-encoded: {form_data}")
                    
                    # Extract fields
                    org_id = int(form_data.get("org_id", ["0"])[0])
                    scope = form_data.get("scope", ["all"])[0]
                    send_mode = form_data.get("send_mode", ["test"])[0]
                    test_email = form_data.get("test_email", [None])[0]
                    contact_ids = form_data.get("contact_ids", [])
                    email_types = form_data.get("email_types", [])
                    chunk_size = int(form_data.get("chunk_size", ["25"])[0])
            except Exception as e:
                logger.error(f"Failed to parse request body: {e}")
                raise HTTPException(status_code=400, detail=f"Unsupported content type and failed to parse body: {e}")
        
        # Log the extracted parameters
        logger.info(f"Extracted parameters: org_id={org_id}, contact_ids_count={len(contact_ids)}, "
                   f"email_types={email_types}, send_mode={send_mode}, scope={scope}")
        
        # Validate required fields
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID is required")
            
        if not contact_ids:
            raise HTTPException(status_code=400, detail="Contact IDs are required")
            
        # Make sure we have at least one email type
        if not email_types:
            raise HTTPException(status_code=400, detail="At least one email type must be selected")
            
        # If this is a bulk send with post_window email type and only that type, use the new mode
        use_single_email_mode = (scope == "bulk" and 
                               "post_window" in email_types and 
                               len(email_types) == 1)
            
        if use_single_email_mode:
            logger.info("Using single email per contact mode for post_window bulk send")
            # Modified initialize_batch that sends one email per contact
            result = batch_manager.initialize_batch_single_email(
                org_id=org_id,
                contact_ids=contact_ids,
                email_type="post_window",  # Only use post_window in this mode
                send_mode=send_mode,
                test_email=test_email
            )
            batch_id = result.get("batch_id")
        else:
            # Standard batch initialization - the revised method returns just the batch_id
            start_time = time.time()
            batch_id = batch_manager.initialize_batch(
                org_id=org_id,
                contact_ids=contact_ids,
                email_types=email_types,
                send_mode=send_mode,
                test_email=test_email,
                scope=scope
            )
            duration = time.time() - start_time
            
            # Get batch status for response
            result = batch_manager.get_batch_status(batch_id)
            result["processing_time"] = f"{duration:.2f}s"
        
        logger.info(f"Batch initialized successfully: {result}")
        
        # For HTMX requests, return HTML fragment
        if is_htmx:
            # Get the full status for the template
            batch_status = batch_manager.get_batch_status(batch_id)
            
            # Convert contact_ids list to comma-separated string for template
            contact_ids_str = ",".join(contact_ids)
            
            # Return the batch_progress partial
            return templates.TemplateResponse(
                "partials/batch_progress.html", 
                {
                    "request": request,
                    "batch_id": batch_id,
                    "org_id": org_id,
                    "send_mode": send_mode,
                    "test_email": test_email,
                    "total": batch_status.get("total", 0),
                    "sent": batch_status.get("sent", 0),
                    "failed": batch_status.get("failed", 0),
                    "pending": batch_status.get("pending", 0),
                    "contact_ids": contact_ids_str,
                    "chunk_size": chunk_size
                }
            )
        else:
            # For regular API requests, return JSON as before
            return JSONResponse(content=result)
    except HTTPException:
        logger.exception("HTTP exception in initialize_batch")
        raise
    except Exception as e:
        logger.exception(f"Unhandled exception in initialize_batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Removed duplicate endpoints for list_batches

@app.get("/api/batch_status/{batch_id}")
async def get_batch_status(batch_id: str):
    """Get the status of a batch."""
    try:
        result = batch_manager.get_batch_status(batch_id)
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/list_batches")
async def list_batches(
    org_id: Optional[int] = None,
    limit: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None, regex="^(pending|sent|failed|skipped)?$")
):
    """
    List recent email batches.
    
    Optional status filter:
    - pending: Only batches with pending emails
    - sent: Only batches with sent emails
    - failed: Only batches with failed emails
    - skipped: Only batches with skipped emails
    - None: All batches
    """
    try:
        result = batch_manager.list_batches(org_id, limit, status)
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing batches: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/get_batches")
async def get_batches(org_id: int):
    """Get a list of batches with pending emails."""
    try:
        # Use the list_batches method with the pending status filter
        batches = batch_manager.list_batches(org_id=org_id, status="pending")
        return JSONResponse(content=batches)
    except Exception as e:
        logger.error(f"Error getting batches: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Fallback endpoint that handles any format of request to initialize_batch
# This is a last resort if other methods fail
@app.post("/api/initialize_batch/fallback")
async def initialize_batch_fallback(request: Request):
    """A fallback endpoint for initializing a batch that accepts any format."""
    logger.info("Using fallback initialize_batch endpoint")
    
    try:
        # Try to get the data in any format we can
        data = None
        content_type = request.headers.get('content-type', '').lower()
        logger.info(f"Fallback endpoint: Content-Type is {content_type}")
        
        if 'application/json' in content_type:
            # Parse as JSON
            data = await request.json()
            logger.info(f"Fallback: Parsed JSON data: {data}")
        elif 'application/x-www-form-urlencoded' in content_type or 'multipart/form-data' in content_type:
            # Parse as form data
            form_data = await request.form()
            data = dict(form_data)
            # Handle multiple values for same key
            for key, value in data.items():
                if isinstance(value, list) and len(value) == 1:
                    data[key] = value[0]
            logger.info(f"Fallback: Parsed form data: {data}")
        else:
            # Try to parse the body directly
            body = await request.body()
            logger.info(f"Fallback: Raw body (first 1000 chars): {body[:1000]}")
            
            # Try to decode as URL-encoded form data
            try:
                decoded_body = body.decode('utf-8')
                logger.info(f"Fallback: Decoded body: {decoded_body}")
                
                # Try parsing as query string
                import urllib.parse
                parsed_qs = urllib.parse.parse_qs(decoded_body)
                data = {k: v[0] if len(v) == 1 else v for k, v in parsed_qs.items()}
                logger.info(f"Fallback: Parsed query string: {data}")
            except Exception as parse_error:
                logger.error(f"Fallback: Error parsing body: {parse_error}")
                raise HTTPException(status_code=400, detail=f"Could not parse request body: {str(parse_error)}")
        
        # If we couldn't get any data, return an error
        if not data:
            raise HTTPException(status_code=400, detail="Could not parse request data in any format")
        
        # Extract the required parameters
        org_id = int(data.get('org_id'))
        contact_ids = data.get('contact_ids', [])
        if isinstance(contact_ids, str):
            contact_ids = [contact_ids]
        
        email_types = data.get('email_types', [])
        if isinstance(email_types, str):
            email_types = [email_types]
        
        send_mode = data.get('send_mode')
        test_email = data.get('test_email')
        scope = data.get('scope', 'all')
        
        logger.info(f"Fallback: Using parameters: org_id={org_id}, contact_ids_count={len(contact_ids)}, "
                   f"email_types={email_types}, send_mode={send_mode}, scope={scope}")
        
        # Initialize the batch - the revised method returns just the batch_id
        start_time = time.time()
        batch_id = batch_manager.initialize_batch(
            org_id=org_id,
            contact_ids=contact_ids,
            email_types=email_types,
            send_mode=send_mode,
            test_email=test_email,
            scope=scope
        )
        duration = time.time() - start_time
        
        # Get batch status for response
        result = batch_manager.get_batch_status(batch_id)
        result["processing_time"] = f"{duration:.2f}s"
        
        logger.info(f"Fallback: Batch initialized successfully: {result}")
        return JSONResponse(content=result)
    except Exception as e:
        logger.exception(f"Fallback: Error initializing batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/process_batch_chunk")
async def process_batch_chunk_endpoint(request: Request):
    """Process a chunk of emails from a batch."""
    try:
        # Check if this is an HTMX request
        is_htmx = request.headers.get("HX-Request") == "true"
        logger.info(f"Received request to process_batch_chunk endpoint (HTMX: {is_htmx})")
        
        # Parse request data based on content type
        content_type = request.headers.get("content-type", "").lower()
        
        if "application/json" in content_type:
            # Parse as JSON
            data = await request.json()
            batch_id = data.get("batch_id")
            chunk_size = int(data.get("chunk_size", 25))
        else:
            # Parse as form data
            form = await request.form()
            batch_id = form.get("batch_id")
            try:
                chunk_size = int(form.get("chunk_size", "25"))
            except ValueError:
                chunk_size = 25
        
        if not batch_id:
            raise HTTPException(status_code=400, detail="Batch ID is required")
            
        start_time = time.time()
        logger.info(f"Processing batch chunk for batch {batch_id}, chunk size {chunk_size}")
        
        # Use the optimized async version for maximum performance
        result = await batch_manager.process_batch_chunk_async(
            batch_id=batch_id,
            chunk_size=chunk_size,
            delay=0.0  # No delay for maximum throughput
        )
        
        # Calculate total processing time
        total_duration = time.time() - start_time
        
        # Add endpoint processing stats to the result
        result["total_endpoint_time"] = f"{total_duration:.2f}s"
        
        # Add human-readable processing rate
        if total_duration > 0 and result["processed"] > 0:
            emails_per_second = result["processed"] / total_duration
            result["processing_rate"] = f"{emails_per_second:.1f} emails/second"
        
        logger.info(f"Batch chunk processed in {total_duration:.2f}s: {result.get('sent', 0)} sent, {result.get('failed', 0)} failed")
        
        # For HTMX requests, return HTML fragment
        if is_htmx:
            # Get the batch status for updating the UI
            batch_status = batch_manager.get_batch_status(batch_id)
            
            # Get org_id for view failed button link
            org_id = batch_status.get("org_id", 0)
            
            # Create a formatted log entry
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_content = (
                f"[{timestamp}] Processed {result.get('processed', 0)} emails in {result.get('total_endpoint_time', '0s')}\n"
                f"Sent: {result.get('sent', 0)}, Failed: {result.get('failed', 0)}, Remaining: {result.get('remaining', 0)}\n"
                f"Processing rate: {result.get('processing_rate', '0 emails/second')}\n"
            )
            
            # Add errors to log if any
            errors = result.get("errors", [])
            if errors:
                log_content += "Errors:\n"
                for error in errors[:5]:  # Limit to first 5 errors
                    log_content += f"- {error}\n"
                if len(errors) > 5:
                    log_content += f"... and {len(errors) - 5} more errors\n"
            
            log_content += "\n"  # Add blank line between entries
            
            return templates.TemplateResponse(
                "partials/chunk_results.html", 
                {
                    "request": request,
                    "batch_id": batch_id,
                    "org_id": org_id,
                    "total": batch_status.get("total", 0),
                    "sent": batch_status.get("sent", 0),
                    "failed": batch_status.get("failed", 0),
                    "pending": batch_status.get("pending", 0),
                    "log_content": log_content,
                    "chunk_size": chunk_size
                }
            )
        else:
            # For regular API requests, return JSON as before
            return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing batch chunk: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/resume_batch")
async def resume_batch(data: BatchResumeParams):
    """Resume a batch by processing the next chunk of pending emails."""
    try:
        # Use the async version for better performance
        result = await batch_manager.resume_batch_async(
            batch_id=data.batch_id,
            chunk_size=data.chunk_size,
            delay=0.1  # Add a small delay between emails
        )
        
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/retry_failed_emails")
async def retry_failed_emails(request: Request):
    """Retry failed emails from a batch."""
    try:
        # Check if this is an HTMX request
        is_htmx = request.headers.get("HX-Request") == "true"
        logger.info(f"Received request to retry_failed_emails endpoint (HTMX: {is_htmx})")
        
        # Parse request data based on content type
        content_type = request.headers.get("content-type", "").lower()
        
        if "application/json" in content_type:
            # Parse as JSON
            data = await request.json()
            batch_id = data.get("batch_id")
            chunk_size = int(data.get("chunk_size", 25))
        else:
            # Parse as form data
            form = await request.form()
            batch_id = form.get("batch_id")
            try:
                chunk_size = int(form.get("chunk_size", "25"))
            except ValueError:
                chunk_size = 25
        
        if not batch_id:
            raise HTTPException(status_code=400, detail="Batch ID is required")
            
        start_time = time.time()
        logger.info(f"Retrying failed emails for batch {batch_id}, chunk size {chunk_size}")
        
        # Use the async version for better performance
        result = await batch_manager.retry_failed_emails_async(
            batch_id=batch_id,
            chunk_size=chunk_size,
            delay=0.1  # Add a small delay between emails
        )
        
        # Calculate total processing time
        total_duration = time.time() - start_time
        result["total_endpoint_time"] = f"{total_duration:.2f}s"
        
        logger.info(f"Retry operation completed in {total_duration:.2f}s: " +
                   f"{result.get('retry_successful', 0)} successful, {result.get('retry_failed', 0)} failed")
        
        # For HTMX requests, return HTML fragment
        if is_htmx:
            # Get the batch status for updating the UI
            batch_status = batch_manager.get_batch_status(batch_id)
            
            # Get org_id for view failed button link
            org_id = batch_status.get("org_id", 0)
            
            # Create a formatted log entry
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_content = (
                f"[{timestamp}] RETRY OPERATION\n"
                f"Retried {result.get('retry_total', 0)} failed emails in {result.get('total_endpoint_time', '0s')}\n"
                f"Successful: {result.get('retry_successful', 0)}, Still Failed: {result.get('retry_failed', 0)}\n"
            )
            
            # Add errors to log if any
            errors = result.get("errors", [])
            if errors:
                log_content += "Errors:\n"
                for error in errors[:5]:  # Limit to first 5 errors
                    log_content += f"- {error}\n"
                if len(errors) > 5:
                    log_content += f"... and {len(errors) - 5} more errors\n"
            
            log_content += "\n"  # Add blank line between entries
            
            return templates.TemplateResponse(
                "partials/chunk_results.html", 
                {
                    "request": request,
                    "batch_id": batch_id,
                    "org_id": org_id,
                    "total": batch_status.get("total", 0),
                    "sent": batch_status.get("sent", 0),
                    "failed": batch_status.get("failed", 0),
                    "pending": batch_status.get("pending", 0),
                    "log_content": log_content,
                    "chunk_size": chunk_size
                }
            )
        else:
            # For regular API requests, return JSON as before
            return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrying failed emails: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Removed duplicate endpoint
# @app.get("/api/list_batches")
# This endpoint was a duplicate of the one at line ~2495

@app.get("/failed_emails", response_class=HTMLResponse)
async def failed_emails(request: Request, batch_id: str, org_id: int):
    """Display detailed information about failed emails in a batch."""
    try:
        # Connect to the organization database
        org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
        
        conn = sqlite3.connect(org_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get batch information
        batch_info = batch_manager.get_batch_status(batch_id)
        
        # Get failed emails for this batch
        cursor.execute("""
            SELECT id, contact_id, email_type, scheduled_date, send_attempt_count, last_attempt_date, last_error
            FROM email_send_tracking
            WHERE batch_id = ? AND send_status = 'failed'
            ORDER BY last_attempt_date DESC
        """, (batch_id,))
        
        failed_emails = []
        for row in cursor.fetchall():
            # Get contact details
            contact_cursor = conn.cursor()
            contact_cursor.execute("""
                SELECT first_name, last_name, email
                FROM contacts
                WHERE id = ?
            """, (row['contact_id'],))
            
            contact = contact_cursor.fetchone()
            contact_name = f"{contact['first_name']} {contact['last_name']}" if contact else "Unknown"
            contact_email = contact['email'] if contact else "Unknown"
            
            failed_emails.append({
                "id": row['id'],
                "contact_id": row['contact_id'],
                "contact_name": contact_name,
                "contact_email": contact_email,
                "email_type": row['email_type'],
                "scheduled_date": row['scheduled_date'],
                "last_attempt_date": row['last_attempt_date'],
                "send_attempt_count": row['send_attempt_count'],
                "last_error": row['last_error']
            })
        
        conn.close()
        
        org_name = get_organization_details(org_id).get('name', f'Organization {org_id}')
        
        return templates.TemplateResponse("failed_emails.html", {
            "request": request,
            "failed_emails": failed_emails,
            "batch_id": batch_id,
            "org_id": org_id,
            "org_name": org_name,
            "batch_info": batch_info
        })
    except Exception as e:
        logger.error(f"Error getting failed emails: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add a button to the send_emails page to go to the batch interface
@app.get("/send_emails_redirect")
async def send_emails_redirect(
    org_id: int,
    contact_ids: List[str] = Query([])
):
    """Redirect to the batch email interface with the selected contacts."""
    contact_ids_params = "&".join([f"contact_ids={contact_id}" for contact_id in contact_ids])
    return RedirectResponse(url=f"/email_batch?org_id={org_id}&{contact_ids_params}")

# Add a specific handler for 400 Bad Request errors
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions and provide detailed logging for 400 errors."""
    # Get request path and method
    path = request.url.path
    method = request.method
    
    # Log the exception with detailed information
    if exc.status_code == 400:
        logger.error(f"400 Bad Request in {method} {path}: {exc.detail}")
        
        # Also log request details that might help with debugging
        try:
            headers = dict(request.headers)
            # Remove sensitive headers
            if "authorization" in headers:
                headers["authorization"] = "[REDACTED]"
            if "cookie" in headers:
                headers["cookie"] = "[REDACTED]"
                
            logger.error(f"Request headers for 400 error: {headers}")
            
            # Log request body if possible
            body = await request.body()
            logger.error(f"Request body for 400 error (first 1000 chars): {body[:1000]}")
        except Exception as log_error:
            logger.error(f"Error logging request details for 400 error: {log_error}")
    else:
        logger.error(f"HTTP {exc.status_code} in {method} {path}: {exc.detail}")
    
    # Return the error with the original status code
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

# Add a global exception handler to catch and log any unhandled exceptions
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler to log all unhandled exceptions."""
    # Get request path and method
    path = request.url.path
    method = request.method
    
    # Log the exception with detailed information
    logger.exception(f"Unhandled exception in {method} {path}: {str(exc)}")
    
    # Also log request details that might help with debugging
    try:
        headers = dict(request.headers)
        # Remove sensitive headers
        if "authorization" in headers:
            headers["authorization"] = "[REDACTED]"
        if "cookie" in headers:
            headers["cookie"] = "[REDACTED]"
            
        logger.error(f"Request headers: {headers}")
        
        # Log request body if possible
        body = await request.body()
        logger.error(f"Request body (first 1000 chars): {body[:1000]}")
    except Exception as log_error:
        logger.error(f"Error logging request details: {log_error}")
    
    # Return a proper error response
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )

# Log all registered routes to help with debugging
@app.on_event("startup")
async def log_routes():
    """Log all registered routes on startup for debugging."""
    routes = []
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            methods = route.methods
            path = route.path
            name = route.name if hasattr(route, "name") else "unnamed"
            routes.append(f"{', '.join(methods)} {path} -> {name}")
    
    # Sort routes alphabetically by path for easier reading
    routes.sort()
    
    logger.info("Registered routes:")
    for route in routes:
        logger.info(f"  {route}")
    
    # Specifically check for our problematic endpoint
    initialize_batch_routes = [r for r in routes if "/api/initialize_batch" in r]
    if initialize_batch_routes:
        logger.info(f"Found initialize_batch routes: {initialize_batch_routes}")
    else:
        logger.warning("No initialize_batch routes found!")

# Main entry point
if __name__ == "__main__":
    import uvicorn
    import argparse
    
    parser = argparse.ArgumentParser(description="Email Scheduler App")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")
    args = parser.parse_args()
    
    # Set up more detailed logging for Uvicorn
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
    
    logger.info("Starting server with enhanced debugging")
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_config=log_config) 