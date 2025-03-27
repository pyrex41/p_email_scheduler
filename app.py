from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
import pandas as pd
import tempfile
import os
from datetime import date
import asyncio
from typing import Optional, List
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
    YEAR_ROUND_ENROLLMENT_STATES
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

@app.post("/resample/{org_id}")
async def resample_contacts(
    org_id: int, 
    sample_size: int = 10,
    state: Optional[str] = None,
    special_rules_only: bool = False
):
    """Resample contacts from existing data"""
    try:
        if org_id not in org_data_store:
            return JSONResponse(
                status_code=404,
                content={"error": "Organization data not found. Please run the initial check first."}
            )
            
        df = org_data_store[org_id]
        
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
        sample_df = filtered_df[filtered_df['contact_id'].isin(sample_ids)]
        
        # Convert DataFrame to list of dicts, handling NaN values
        sample_data = sample_df.replace({pd.NA: None}).to_dict('records')
        
        # Group data by contact
        contacts_data = {}
        for row in sample_data:
            contact_id = row['contact_id']
            if contact_id not in contacts_data:
                state_code = row['state']
                state_info = {
                    "code": state_code,
                    "has_birthday_rule": state_code in BIRTHDAY_RULE_STATES,
                    "has_effective_date_rule": state_code in EFFECTIVE_DATE_RULE_STATES,
                    "has_year_round_enrollment": state_code in YEAR_ROUND_ENROLLMENT_STATES
                }
                
                contacts_data[contact_id] = {
                    'contact_info': {
                        'id': contact_id,
                        'name': f"{row['first_name']} {row['last_name']}",
                        'email': row['email'],
                        'state': state_code,
                        'state_info': state_info,
                        'birth_date': row['birth_date'] if pd.notna(row['birth_date']) else None,
                        'effective_date': row['effective_date'] if pd.notna(row['effective_date']) else None
                    },
                    'emails': []
                }
            contacts_data[contact_id]['emails'].append({
                'type': row['email_type'],
                'date': row['email_date'],
                'link': row['link'] if pd.notna(row['link']) else '',
                'skipped': row['skipped'],
                'reason': row['reason'] if pd.notna(row['reason']) else ''
            })
            
        return {
            "contacts": contacts_data,
            "total_contacts": len(unique_contacts),
            "sample_size": len(sample_ids)
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
    special_rules_only: bool = Form(default=False)
):
    """Process organization's contacts and display sample results"""
    try:
        # Set up paths
        main_db = "main.db"
        org_db_dir = "org_dbs"

        await refresh_databases(org_id)
        
        # Get organization details
        org = get_organization_details(main_db, org_id)
        
        # Get contacts from organization database
        org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
        contacts = get_contacts_from_org_db(org_db_path, org_id)
        
        # Format contact data
        formatted_contacts = format_contact_data(contacts)
        
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
        results = await process_contacts_async(formatted_contacts, current_date, end_date)
        
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
            # Write results to CSV
            from schedule_org_emails import write_results_to_csv
            write_results_to_csv(results, formatted_contacts, org_id, tmp.name)
            
            # Read the CSV with pandas
            df = pd.read_csv(tmp.name)
            
            # Store the DataFrame in memory
            org_data_store[org_id] = df
        
        # Clean up temp file
        os.unlink(tmp.name)
        
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
        sample_df = filtered_df[filtered_df['contact_id'].isin(sample_ids)]
        
        # Convert DataFrame to list of dicts for template
        sample_data = sample_df.to_dict('records')
        
        # Group data by contact
        contacts_data = {}
        for row in sample_data:
            contact_id = row['contact_id']
            if contact_id not in contacts_data:
                state_code = row['state']
                state_info = {
                    "code": state_code,
                    "has_birthday_rule": state_code in BIRTHDAY_RULE_STATES,
                    "has_effective_date_rule": state_code in EFFECTIVE_DATE_RULE_STATES,
                    "has_year_round_enrollment": state_code in YEAR_ROUND_ENROLLMENT_STATES
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
                    'emails': []
                }
            contacts_data[contact_id]['emails'].append({
                'type': row['email_type'],
                'date': row['email_date'],
                'link': row['link'],
                'skipped': row['skipped'],
                'reason': row['reason']
            })
        
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "org_name": org['name'],
                "org_id": org_id,
                "contacts": contacts_data,
                "total_contacts": len(unique_contacts),
                "sample_size": sample_size,
                "sample_sizes": [5, 10, 25, 50, 100],
                "selected_state": state if state and state.strip() else None,
                "special_rules_only": special_rules_only,
                "all_states": ALL_STATES,
                "special_rule_states": SPECIAL_RULE_STATES
            }
        )
        
    except Exception as e:
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error": str(e)
            }
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 