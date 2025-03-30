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

# Import our email scheduling code
from schedule_org_emails import (
    get_organization_details,
    get_contacts_from_org_db,
    format_contact_data,
    process_contacts_async,
    BIRTHDAY_RULE_STATES,
    EFFECTIVE_DATE_RULE_STATES,
    YEAR_ROUND_ENROLLMENT_STATES,
    write_results_to_csv
)

from email_scheduler_common import (
    calculate_birthday_email_date, 
    calculate_effective_date_email, 
    get_aep_dates_for_year,
    DateRange,
    calculate_post_window_dates,
    calculate_rule_windows,
    calculate_exclusion_periods,
    is_date_excluded,
    get_all_occurrences
)

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

# Get list of states with special rules
SPECIAL_RULE_STATES = sorted(set(
    list(BIRTHDAY_RULE_STATES.keys()) + 
    list(EFFECTIVE_DATE_RULE_STATES.keys()) + 
    list(YEAR_ROUND_ENROLLMENT_STATES)
))

# All US states
ALL_STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC'
]

# Load ZIP code data
with open('zipData.json') as f:
    ZIP_DATA = json.load(f)

def get_state_from_zip(zip_code: str) -> str:
    """
    Get state from ZIP code using zipData.json
    
    Args:
        zip_code: ZIP code as string
        
    Returns:
        Two-letter state code, or None if not found
    """
    try:
        if not zip_code or not str(zip_code).strip():
            return None
        # Convert to string and take first 5 digits
        zip_str = str(zip_code)[:5]
        if zip_str in ZIP_DATA:
            return ZIP_DATA[zip_str]['state']
    except (KeyError, TypeError, ValueError):
        pass
    return None

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
        # Parse dates
        birth_date = datetime.strptime(data.birth_date, "%Y-%m-%d").date()
        start_date = datetime.strptime(data.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(data.end_date, "%Y-%m-%d").date()
        
        effective_date = None
        if data.effective_date:
            effective_date = datetime.strptime(data.effective_date, "%Y-%m-%d").date()
            
        # Set up contact data
        contact = {
            "id": "12345",  # Dummy ID
            "birth_date": birth_date,
            "effective_date": effective_date,
            "state": data.state
        }
        
        # Calculate birthdays in range
        birthdays = []
        if birth_date:
            birthdays = get_all_occurrences(birth_date, start_date, end_date)
        
        # Calculate effective dates in range
        effective_dates = []
        if effective_date:
            effective_dates = get_all_occurrences(effective_date, start_date, end_date)
        
        # Get AEP dates
        aep_dates = []
        if data.state not in YEAR_ROUND_ENROLLMENT_STATES:
            for year in range(start_date.year, end_date.year + 1):
                year_aep_dates = get_aep_dates_for_year(year)
                aep_dates.extend([d for d in year_aep_dates if start_date <= d <= end_date])
        
        # Calculate rule windows
        rule_windows = calculate_rule_windows(contact, birthdays, effective_dates, start_date, end_date)
        
        # Calculate exclusion periods
        exclusion_periods = calculate_exclusion_periods(rule_windows, start_date, end_date)
        
        # Calculate post-window dates - only for states with specific rules
        post_window_dates = []
        if rule_windows and (data.state in BIRTHDAY_RULE_STATES or data.state in EFFECTIVE_DATE_RULE_STATES):
            post_window_dates = calculate_post_window_dates(rule_windows, end_date)
        
        # Schedule emails
        scheduled_emails = []
        
        # Check if this is a year-round enrollment state
        if data.state in YEAR_ROUND_ENROLLMENT_STATES:
            # No emails for year-round enrollment states
            return {
                "emails": [],
                "exclusion_periods": [
                    {"start_date": period.start.isoformat(), "end_date": period.end_date.isoformat(), 
                     "type": "Year-Round Enrollment"} 
                    for period in exclusion_periods
                ],
                "birthdays": [d.isoformat() for d in birthdays],
                "effective_dates": [d.isoformat() for d in effective_dates],
                "aep_dates": [],
                "state": data.state,
                "message": "No emails scheduled - year-round enrollment state"
            }
        
        # Birthday emails
        if birthdays:
            for birthday in birthdays:
                email_date = calculate_birthday_email_date(birthday, birthday.year)
                
                # Only include if within date range
                if start_date <= email_date <= end_date:
                    # For birthday emails, states without specific birthday rules 
                    # (not in BIRTHDAY_RULE_STATES) should always get emails
                    bypass_exclusion = data.state not in BIRTHDAY_RULE_STATES
                    
                    # States without rule windows will have empty exclusion_periods list,
                    # so is_date_excluded will return False
                    if bypass_exclusion or not is_date_excluded(email_date, exclusion_periods):
                        scheduled_emails.append({
                            "type": "birthday",
                            "date": email_date.isoformat(),
                            "reason": f"14 days before birthday ({birthday.isoformat()})"
                        })
        
        # Effective date emails
        if effective_dates:
            for eff_date in effective_dates:
                email_date = calculate_effective_date_email(eff_date, start_date)
                
                # Only include if within date range
                if start_date <= email_date <= end_date:
                    # For effective date emails, states with effective date rules (like Missouri)
                    # should always get these emails
                    bypass_exclusion = data.state in EFFECTIVE_DATE_RULE_STATES
                    
                    # States without rule windows will have empty exclusion_periods list,
                    # so is_date_excluded will return False
                    if bypass_exclusion or not is_date_excluded(email_date, exclusion_periods):
                        scheduled_emails.append({
                            "type": "effective_date",
                            "date": email_date.isoformat(),
                            "reason": f"30 days before effective date ({eff_date.isoformat()})"
                        })
        
        # AEP emails
        if aep_dates and data.state not in YEAR_ROUND_ENROLLMENT_STATES:
            # Distribute contact across AEP weeks
            contact_index = 12345 % len(aep_dates)  # Use dummy contact ID
            aep_date = aep_dates[contact_index]
            
            # States without rule windows should always get AEP emails
            # States without rule windows will have empty exclusion_periods list,
            # so is_date_excluded will return False
            if not is_date_excluded(aep_date, exclusion_periods):
                scheduled_emails.append({
                    "type": "aep",
                    "date": aep_date.isoformat(),
                    "reason": "Annual Enrollment Period email"
                })
        
        # Post-window emails
        if post_window_dates:
            # We only send one post-window email, so use the earliest one
            post_date = post_window_dates[0]
            
            # Post-window emails bypass exclusion periods
            scheduled_emails.append({
                "type": "post_window",
                "date": post_date.isoformat(),
                "reason": "Day after exclusion period"
            })
        
        # Sort emails by date
        scheduled_emails.sort(key=lambda x: x["date"])
        
        return {
            "emails": scheduled_emails,
            "exclusion_periods": [
                {"start_date": period.start.isoformat(), "end_date": period.end_date.isoformat()} 
                for period in exclusion_periods
            ],
            "birthdays": [d.isoformat() for d in birthdays],
            "effective_dates": [d.isoformat() for d in effective_dates],
            "aep_dates": [d.isoformat() for d in aep_dates],
            "state": data.state
        }
        
    except Exception as e:
        # Log the error and return an error response
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in simulate_emails: {e}\n{error_trace}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
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
        if org_id not in org_data_store:
            return JSONResponse(
                status_code=404,
                content={"error": "Organization data not found. Please run the initial check first."}
            )
            
        df = org_data_store[org_id]
        
        # Apply contact search if provided
        if contact_search and contact_search.strip():
            search_term = contact_search.strip()
            # Search by email (case insensitive) or by contact ID
            filtered_df = df[(df['email'].str.lower() == search_term.lower()) | 
                             (df['contact_id'].astype(str) == search_term)]
            
            if len(filtered_df) == 0:
                return JSONResponse(
                    status_code=404,
                    content={"error": f"No contact found with email or ID: {search_term}"}
                )
        else:
            # Apply state filtering
            filtered_df = df.copy()
            if special_rules_only:
                filtered_df = filtered_df[filtered_df['state'].isin(SPECIAL_RULE_STATES)]
            elif state and state.strip():  # Only filter if state is explicitly selected
                filtered_df = filtered_df[filtered_df['state'] == state]
                
            # Get unique contacts with their states
            unique_contacts = filtered_df.groupby('contact_id').first().reset_index()
            
            if len(unique_contacts) == 0:
                return JSONResponse(
                    status_code=404,
                    content={"error": "No contacts found matching the state filter criteria."}
                )
            
            # Sample contacts ensuring good state distribution
            sample_ids = sample_contacts_from_states(unique_contacts, sample_size, state if state and state.strip() else None)
            
            # Filter dataframe to only include sampled contacts
            filtered_df = filtered_df[filtered_df['contact_id'].isin(sample_ids)]
        
        # Convert DataFrame to list of dicts, handling NaN values
        sample_data = filtered_df.replace({pd.NA: None}).to_dict('records')
        
        # Group data by contact with improved organization
        contacts_data = {}
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
                
                contacts_data[contact_id] = {
                    'contact_info': {
                        'id': contact_id,
                        'name': f"{row['first_name']} {row['last_name']}",
                        'email': row['email'],
                        'state': state_code,
                        'state_info': state_info,
                        'birth_date': row['birth_date'],
                        'effective_date': row['effective_date']
                    },
                    'scheduled_emails': {
                        'birthday': [],
                        'effective_date': [],
                        'aep': [],
                        'post_window': []
                    },
                    'skipped_emails': [],
                    'scheduling_rules': []
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
            
            # Add email to appropriate category
            if not row['skipped']:
                email_type = row['email_type']
                email_info = {
                    'date': str(row['email_date']),
                    'link': row['link'],
                    'reason': row['reason'] if row['reason'] else None
                }
                contacts_data[contact_id]['scheduled_emails'][email_type].append(email_info)
            else:
                contacts_data[contact_id]['skipped_emails'].append({
                    'type': row['email_type'],
                    'reason': row['reason']
                })
            
        return {
            "contacts": contacts_data,
            "total_contacts": len(df.groupby('contact_id')),
            "sample_size": len(contacts_data),
            "contact_search": contact_search if contact_search else ""
        }
        
    except Exception as e:
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
        # Set up paths
        main_db = "main.db"
        org_db_dir = "org_dbs"

        await refresh_databases(org_id)
        
        # Get organization details
        try:
            org = get_organization_details(main_db, org_id)
        except Exception as e:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to get organization details: {str(e)}"
                }
            )
        
        # Get contacts from organization database
        try:
            org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
            contacts = get_contacts_from_org_db(org_db_path, org_id)
        except Exception as e:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to get contacts from database: {str(e)}"
                }
            )
        
        # Format contact data
        try:
            formatted_contacts = format_contact_data(contacts)
        except Exception as e:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to format contact data: {str(e)}\nLocation: format_contact_data"
                }
            )
        
        if not formatted_contacts:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": "No valid contacts found for scheduling"
                }
            )
        
        # Set date range
        current_date = date.today()
        end_date = date(current_date.year + 2, current_date.month, current_date.day)
        
        # Process contacts
        try:
            results = await process_contacts_async(formatted_contacts, current_date, end_date)
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
        
        # Create a temporary CSV file
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
                # Write results to CSV
                write_results_to_csv(results, formatted_contacts, org_id, tmp.name)
                
                # Read the CSV with pandas
                df = pd.read_csv(tmp.name)
                
                # Store the DataFrame in memory
                org_data_store[org_id] = df
        except Exception as e:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to create CSV file: {str(e)}"
                }
            )
            
        # Clean up temp file
        os.unlink(tmp.name)
        
        # Apply contact search if provided
        try:
            if contact_search and contact_search.strip():
                search_term = contact_search.strip()
                # Search by email (case insensitive) or by contact ID
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
                # Apply state filtering
                filtered_df = df.copy()
                if special_rules_only:
                    filtered_df = filtered_df[filtered_df['state'].isin(SPECIAL_RULE_STATES)]
                elif state and state.strip():  # Only filter if state is explicitly selected
                    filtered_df = filtered_df[filtered_df['state'] == state]
                    
                # Get unique contacts with their states
                unique_contacts = filtered_df.groupby('contact_id').first().reset_index()
                
                if len(unique_contacts) == 0:
                    return templates.TemplateResponse(
                        "error.html",
                        {
                            "request": request,
                            "error": "No contacts found matching the state filter criteria."
                        }
                    )
                
                # Sample contacts ensuring good state distribution
                sample_ids = sample_contacts_from_states(unique_contacts, sample_size, state if state and state.strip() else None)
                
                # Filter dataframe to only include sampled contacts
                filtered_df = filtered_df[filtered_df['contact_id'].isin(sample_ids)]
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed during contact filtering/sampling: {str(e)}\nTrace:\n{error_trace}"
                }
            )
        
        # Convert DataFrame to list of dicts for template
        try:
            sample_data = filtered_df.to_dict('records')
            
            # Group data by contact with improved organization
            contacts_data = {}
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
                    
                    contacts_data[contact_id] = {
                        'contact_info': {
                            'id': contact_id,
                            'name': f"{row['first_name']} {row['last_name']}",
                            'email': row['email'],
                            'state': state_code,
                            'state_info': state_info,
                            'birth_date': row['birth_date'],
                            'effective_date': row['effective_date']
                        },
                        'scheduled_emails': {
                            'birthday': [],
                            'effective_date': [],
                            'aep': [],
                            'post_window': []
                        },
                        'skipped_emails': [],
                        'scheduling_rules': [],
                        'emails': []  # Keep for backwards compatibility with the template
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
                
                # Add email to appropriate category and to the backward compatibility list
                email_info = {
                    'type': row['email_type'],
                    'date': str(row['email_date']),
                    'link': row['link'],
                    'skipped': row['skipped'],
                    'reason': row['reason']
                }
                
                # Add to the emails list for backwards compatibility
                contacts_data[contact_id]['emails'].append(email_info)
                
                # Also add to the structured format
                if row['skipped'] != 'Yes':
                    email_type = row['email_type'].lower()
                    if email_type in contacts_data[contact_id]['scheduled_emails']:
                        contacts_data[contact_id]['scheduled_emails'][email_type].append({
                            'date': str(row['email_date']),
                            'link': row['link'],
                            'reason': row['reason']
                        })
                else:
                    contacts_data[contact_id]['skipped_emails'].append({
                        'type': row['email_type'],
                        'reason': row['reason']
                    })
                
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": f"Failed to prepare contact data for display: {str(e)}\nTrace:\n{error_trace}"
                }
            )
        
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
                "selected_state": state if state and state.strip() else None,
                "special_rules_only": special_rules_only,
                "all_states": ALL_STATES,
                "special_rule_states": SPECIAL_RULE_STATES,
                "contact_search": contact_search if contact_search else ""
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