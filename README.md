# Email Scheduler

A FastAPI web application for scheduling and checking Medicare-related emails based on complex business rules.

## Features

- Web interface for checking email schedules for an organization's contacts
- Sample contacts based on state and special rule criteria
- Support for state-specific email rules (birthday rules, effective date rules, etc.)
- Detailed display of scheduled emails with timing information
- Integration with existing email scheduling backend

## Technical Stack

- FastAPI for the web backend
- Jinja2 for templating
- SQLite for data storage
- Async processing for handling large contact datasets
- Bootstrap for the frontend interface

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the application:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```

## Usage

1. Enter an organization ID on the home page
2. Select sample size and optional state filter
3. View the scheduled emails for a random sample of contacts
4. Resample to see different contacts from the organization

## State-Specific Rules

The application handles special Medicare rules for various states:
- Birthday rule states (CA, ID, IL, KY, LA, MD, NV, OK, OR)
- Effective date rule states (MO)
- Year-round enrollment states