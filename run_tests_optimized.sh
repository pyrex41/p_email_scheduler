#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test directory and database settings
TEST_DIR="./temp_test"
MAIN_DB="$TEST_DIR/main.db"
OUTPUT_DIR="$TEST_DIR/output"
SYNC_OUTPUT="$OUTPUT_DIR/sync_output.json"
ASYNC_OUTPUT="$OUTPUT_DIR/async_output.json"
OPTIMIZED_OUTPUT="$OUTPUT_DIR/optimized_output.json"
TEST_CASES="test_cases.json"

# Create directories if they don't exist
mkdir -p "$TEST_DIR"
mkdir -p "$OUTPUT_DIR"

echo "Setting up test environment..."

# Create main database
echo "Creating main database..."

# Create a temporary Python script for database creation
cat > create_main_db.py << EOF
import sqlite3
conn = sqlite3.connect('$MAIN_DB')
conn.execute('CREATE TABLE IF NOT EXISTS contacts (id INTEGER PRIMARY KEY, name TEXT, birth_date TEXT, effective_date TEXT, age INTEGER, state TEXT, organization_id INTEGER)')
conn.execute('CREATE TABLE IF NOT EXISTS organizations (id INTEGER PRIMARY KEY, name TEXT)')

# Insert some test organizations
for org_id in range(1, 8):
    conn.execute('INSERT OR REPLACE INTO organizations (id, name) VALUES (?, ?)', 
                (org_id, f'Test Organization {org_id}'))

conn.commit()
conn.close()
print('Main database created successfully with organizations table')
EOF

# Run the script
./run_with_uv.sh create_main_db.py
rm create_main_db.py

# Create test organization databases
echo "Creating organization databases from test cases..."

# Create a temporary Python script for organization database creation
cat > create_org_dbs.py << EOF
import json
import sqlite3
import os

# Load test cases
with open('$TEST_CASES', 'r') as f:
    test_data = json.load(f)

# Create databases for each organization
for test_name, test_info in test_data['test_cases'].items():
    org_id = test_info['org_id']
    contacts = test_info['contacts']
    
    db_path = os.path.join('$TEST_DIR', f'org_{org_id}.db')
    print(f'Creating database for organization {org_id}...')
    
    # Create database
    conn = sqlite3.connect(db_path)
    conn.execute('CREATE TABLE IF NOT EXISTS contacts (id INTEGER PRIMARY KEY, name TEXT, birth_date TEXT, effective_date TEXT, age INTEGER, state TEXT)')
    
    # Insert contacts
    for contact in contacts:
        conn.execute(
            'INSERT OR REPLACE INTO contacts (id, birth_date, effective_date, age, state) VALUES (?, ?, ?, ?, ?)',
            (contact['id'], contact['birth_date'], contact['effective_date'], contact['age'], contact['state'])
        )
        
        # Also add to main database with organization_id
        main_conn = sqlite3.connect('$MAIN_DB')
        main_conn.execute(
            'INSERT OR REPLACE INTO contacts (id, birth_date, effective_date, age, state, organization_id) VALUES (?, ?, ?, ?, ?, ?)',
            (contact['id'], contact['birth_date'], contact['effective_date'], contact['age'], contact['state'], org_id)
        )
        main_conn.commit()
        main_conn.close()
    
    conn.commit()
    conn.close()
EOF

# Run the script
./run_with_uv.sh create_org_dbs.py
rm create_org_dbs.py

# Convert database to JSON for optimized version
echo "Converting database to JSON format for optimized testing..."

# Create a temporary Python script for JSON conversion
cat > convert_to_json.py << EOF
import sqlite3
import json

# Connect to the main database
conn = sqlite3.connect('$MAIN_DB')
conn.row_factory = sqlite3.Row

# Get all contacts
cursor = conn.execute('SELECT * FROM contacts')
contacts = [dict(row) for row in cursor.fetchall()]

# Create a consistent format for all implementations
formatted_contacts = []
for contact in contacts:
    formatted_contacts.append({
        'id': contact['id'],
        'contact_id': str(contact['id']),  # Ensure string format for IDs
        'birth_date': contact['birth_date'],
        'effective_date': contact['effective_date'],
        'age': contact['age'],
        'state': contact['state'],
        'organization_id': contact['organization_id']
    })

# Write to a JSON file
with open('$TEST_DIR/contacts.json', 'w') as f:
    json.dump(formatted_contacts, f, indent=2)

print(f'Converted {len(formatted_contacts)} contacts to JSON format')
EOF

# Run the script
./run_with_uv.sh convert_to_json.py
rm convert_to_json.py

# Run the tests
echo "Running tests..."

# Modify main.py to use the correct database path
echo "Modifying database paths for main.py and async_scheduler.py"

# Create a temporary Python script for path updates
cat > update_paths.py << EOF
import re

# Update main.py
with open('main.py', 'r') as f:
    main_content = f.read()

# Find the database path setting
db_pattern = r'DATABASE_PATH\s*=\s*[\'\"](.*?)[\'\"]'
modified_main = re.sub(db_pattern, f'DATABASE_PATH = \"$MAIN_DB\"', main_content)

# Find the organization database path pattern
org_db_pattern = r'ORG_DATABASE_PATH\s*=\s*[\'\"](.*?)[\'\"]'
modified_main = re.sub(org_db_pattern, f'ORG_DATABASE_PATH = \"$TEST_DIR/org_{{}}.db\"', modified_main)

with open('main.py', 'w') as f:
    f.write(modified_main)

# Update async_scheduler.py
with open('async_scheduler.py', 'r') as f:
    async_content = f.read()

# Find the database path setting
db_pattern = r'DATABASE_PATH\s*=\s*[\'\"](.*?)[\'\"]'
modified_async = re.sub(db_pattern, f'DATABASE_PATH = \"$MAIN_DB\"', async_content)

# Find the organization database path pattern
org_db_pattern = r'ORG_DATABASE_PATH\s*=\s*[\'\"](.*?)[\'\"]'
modified_async = re.sub(org_db_pattern, f'ORG_DATABASE_PATH = \"$TEST_DIR/org_{{}}.db\"', modified_async)

with open('async_scheduler.py', 'w') as f:
    f.write(modified_async)

print('Database paths updated in main.py and async_scheduler.py')
EOF

# Run the script
./run_with_uv.sh update_paths.py
rm update_paths.py

# Test the standard synchronous version
echo "Testing synchronous version..."

# Create a temporary Python script for the synchronous test
cat > run_sync_test.py << EOF
import json
import sys
import os
from datetime import datetime, date

# Create a wrapper to run main.py with correct arguments
start_date_str = '2024-01-01'
start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
output_file = '$SYNC_OUTPUT'

try:
    # Import main module directly
    sys.path.insert(0, os.getcwd())
    from main import schedule_emails
    
    # Override constants if needed
    import main
    main.DEBUG = True
    main.VERBOSE = True
    
    # Run the scheduler
    results = schedule_emails(start_date=start_date)
    
    # Format results for comparison
    formatted_results = []
    for org_id, org_data in results.items():
        for contact_id, contact_data in org_data['scheduled_by_contact'].items():
            entry = {
                'contact_id': contact_id,
                'emails': contact_data['scheduled'],
                'skipped': contact_data['skipped']
            }
            formatted_results.append(entry)
    
    # Write to output file
    with open(output_file, 'w') as f:
        json.dump(formatted_results, f, indent=2)
    
    print(f'Successfully processed {len(formatted_results)} contacts')
except Exception as e:
    print(f'Error running main.py: {e}')
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
EOF

# Run the script
./run_with_uv.sh run_sync_test.py
rm run_sync_test.py

# Test the standard asynchronous version
echo "Testing asynchronous version..."

# Create a temporary Python script for the asynchronous test
cat > run_async_test.py << EOF
import json
import sys
import os
import asyncio
from datetime import datetime, date

# Create a wrapper to run async_scheduler.py with correct arguments
start_date_str = '2024-01-01'
start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
output_file = '$ASYNC_OUTPUT'

try:
    # Import async module directly
    sys.path.insert(0, os.getcwd())
    from async_scheduler import schedule_emails
    
    # Override constants if needed
    import async_scheduler
    async_scheduler.DEBUG = True
    async_scheduler.VERBOSE = True
    
    # Run the scheduler
    results = asyncio.run(schedule_emails(start_date=start_date))
    
    # Format results for comparison
    formatted_results = []
    for org_id, org_data in results.items():
        for contact_id, contact_data in org_data['scheduled_by_contact'].items():
            entry = {
                'contact_id': contact_id,
                'emails': contact_data['scheduled'],
                'skipped': contact_data['skipped']
            }
            formatted_results.append(entry)
    
    # Write to output file
    with open(output_file, 'w') as f:
        json.dump(formatted_results, f, indent=2)
    
    print(f'Successfully processed {len(formatted_results)} contacts')
except Exception as e:
    print(f'Error running async_scheduler.py: {e}')
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
EOF

# Run the script
./run_with_uv.sh run_async_test.py
rm run_async_test.py

# Fix calculate_post_window_dates import in email_rules_engine.py
echo "Fixing imports and functions in email_rules_engine.py..."

# Create a temporary file with the Python code to avoid shell interpretation issues
cat > fix_imports.py << 'EOF'
import sys
import os

try:
    # Add working directory to the path
    sys.path.insert(0, os.getcwd())
    
    # First, ensure function is properly imported
    import email_scheduler_common
    if hasattr(email_scheduler_common, 'calculate_post_window_dates'):
        print('calculate_post_window_dates function exists in email_scheduler_common module')
    else:
        print('ERROR: calculate_post_window_dates function NOT found in email_scheduler_common module')
    
    # Check for calculate_post_window_dates in imports
    with open('email_rules_engine.py', 'r') as f:
        content = f.read()
        
    if 'calculate_post_window_dates' not in content:
        print('Adding calculate_post_window_dates import to email_rules_engine.py')
        with open('email_rules_engine.py', 'r') as f:
            lines = f.readlines()
        
        # Find the import section
        for i, line in enumerate(lines):
            if line.startswith('from email_scheduler_common import ('):
                # Find the closing parenthesis
                for j in range(i+1, len(lines)):
                    if ')' in lines[j]:
                        if 'calculate_post_window_dates' not in ''.join(lines[i:j+1]):
                            # Add the import
                            lines[j] = lines[j].replace(')', ', calculate_post_window_dates)')
                            with open('email_rules_engine.py', 'w') as f:
                                f.writelines(lines)
                            print('Successfully added calculate_post_window_dates to imports')
                        break
                break
    
    # Fix the lambda signature
    if 'lambda contact, date_obj:' in content:
        print('Fixing lambda signature for date_obj lambda in email_rules_engine.py')
        with open('email_rules_engine.py', 'r') as f:
            lines = f.readlines()
        
        # Fix any lambda that has the wrong signature
        for i, line in enumerate(lines):
            if 'lambda contact, date_obj:' in line:
                lines[i] = lines[i].replace('lambda contact, date_obj:', 'lambda contact, current_date, end_date: handle_first_of_month(contact, current_date)')
                print(f'Fixed line {i+1}: {lines[i].strip()}')
        
        with open('email_rules_engine.py', 'w') as f:
            f.writelines(lines)
    
    print('Checks completed')
except Exception as e:
    print(f'Error checking email_rules_engine.py: {e}')
    import traceback
    print(traceback.format_exc())
EOF

# Run the script using our helper
./run_with_uv.sh fix_imports.py

# Remove the temporary file
rm fix_imports.py

# Test the optimized implementation
echo "Testing optimized version..."
uv run python email_scheduler_optimized.py \
    --input "$TEST_DIR/contacts.json" \
    --output "$OPTIMIZED_OUTPUT" \
    --start-date "2024-01-01" \
    --end-date "2024-12-31" \
    --async

# Test the SendGrid integration using mocks
echo "Testing SendGrid integration with mocks..."
uv run python test_sendgrid_integration.py --input "$TEST_DIR/contacts.json"

# Count the number of mock emails "sent"
EMAIL_COUNT=$(find ./mock_emails -name "mock_email_*.json" | wc -l)
if [ "$EMAIL_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ SendGrid integration test successful! $EMAIL_COUNT mock emails generated.${NC}"
else
    echo -e "${RED}❌ SendGrid integration test failed! No mock emails were generated.${NC}"
fi

# Compare results
echo "Comparing results..."

# Create a temporary file with the Python code to avoid shell interpretation issues
cat > compare_results.py << EOF
import json
import os

try:
    # First check if files exist
    sync_output_file = '$SYNC_OUTPUT'
    async_output_file = '$ASYNC_OUTPUT'
    optimized_output_file = '$OPTIMIZED_OUTPUT'
    
    files_exist = True
    for file_path in [sync_output_file, async_output_file, optimized_output_file]:
        if not os.path.exists(file_path):
            print(f'WARNING: Output file does not exist: {file_path}')
            files_exist = False
    
    if not files_exist:
        print('\nCannot compare results because one or more output files are missing.\n')
    else:
        # Compare sync and async outputs
        print('Loading sync results...')
        with open(sync_output_file, 'r') as f:
            sync_data = json.load(f)

        print('Loading async results...')
        with open(async_output_file, 'r') as f:
            async_data = json.load(f)
            
        print('Loading optimized results...')
        with open(optimized_output_file, 'r') as f:
            optimized_data = json.load(f)

        # Sort data by contact_id for consistent comparison
        sync_data = sorted(sync_data, key=lambda x: x.get('contact_id', ''))
        async_data = sorted(async_data, key=lambda x: x.get('contact_id', ''))
        optimized_data = sorted(optimized_data, key=lambda x: x.get('contact_id', ''))

        # Compare raw outputs
        sync_vs_async_match = (json.dumps(sync_data, sort_keys=True) == json.dumps(async_data, sort_keys=True))
        sync_vs_optimized_match = (json.dumps(sync_data, sort_keys=True) == json.dumps(optimized_data, sort_keys=True))
        async_vs_optimized_match = (json.dumps(async_data, sort_keys=True) == json.dumps(optimized_data, sort_keys=True))

        print('')
        if sync_vs_async_match:
            print('✅ Sync and async outputs are identical!')
        else:
            print('❌ Sync and async outputs differ!')
            
        if sync_vs_optimized_match:
            print('✅ Sync and optimized outputs are identical!')
        else:
            print('❌ Sync and optimized outputs differ!')
            
        if async_vs_optimized_match:
            print('✅ Async and optimized outputs are identical!')
        else:
            print('❌ Async and optimized outputs differ!')
    
    print('')
    print('Test comparison completed successfully!')
    
except Exception as e:
    print(f'Error comparing results: {e}')
    import traceback
    print(traceback.format_exc())
EOF

# Run the script using our helper
./run_with_uv.sh compare_results.py

# Remove the temporary file
rm compare_results.py

# Ask about cleaning up
echo ""
echo "Tests completed. Would you like to clean up the test databases? (y/n)"
read -n 1 -r -t 3
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleaning up test environment..."
    rm -rf "$TEST_DIR"
    echo "Test environment cleaned up."
else
    echo "Keeping test databases in $TEST_DIR for inspection."
fi