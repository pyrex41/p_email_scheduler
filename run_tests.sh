#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

echo -e "${BLUE}Setting up test environment...${NC}"

# Create directories if they don't exist
mkdir -p temp_test
mkdir -p temp_test/output

# Create main database with test organizations
echo "Creating main database..."
sqlite3 temp_test/main.db <<EOF
DROP TABLE IF EXISTS organizations;
CREATE TABLE organizations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO organizations (id, name) VALUES
(1, 'Year Transition Test Org'),
(2, 'Age Rule Test Org'),
(3, 'Nevada Month Start Test Org'),
(4, 'Year Round States Test Org'),
(5, 'AEP Exclusion Test Org'),
(6, 'Multiple Emails Test Org'),
(7, 'Leap Year Tests Org');
EOF

# Function to create org database and populate it with contacts
create_org_db() {
    local org_id=$1
    local contacts=$2
    
    echo "Creating database for organization $org_id..."
    sqlite3 "temp_test/org_${org_id}.db" <<EOF
DROP TABLE IF EXISTS contacts;
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL DEFAULT 'Test',
    last_name TEXT NOT NULL DEFAULT 'User',
    email TEXT NOT NULL DEFAULT 'test@example.com',
    current_carrier TEXT NOT NULL DEFAULT 'TestCarrier',
    plan_type TEXT NOT NULL DEFAULT 'N',
    effective_date TEXT NOT NULL,
    birth_date TEXT NOT NULL,
    tobacco_user BOOLEAN NOT NULL DEFAULT 0,
    gender TEXT NOT NULL DEFAULT 'U',
    state TEXT NOT NULL,
    zip_code TEXT NOT NULL DEFAULT '00000'
);

$contacts
EOF
}

# Extract contacts from test_cases.json and create org databases
echo "Creating organization databases from test cases..."
python3 - <<EOF
import json
import sqlite3
import os

# Create temp directory if it doesn't exist
os.makedirs('temp_test', exist_ok=True)

with open('test_cases.json') as f:
    test_cases = json.load(f)['test_cases']

# Also create a combined JSON file for the optimized version
all_contacts = []

for test_case in test_cases.values():
    org_id = test_case['org_id']
    contacts = test_case['contacts']
    
    # Generate SQL INSERT statements
    inserts = []
    for contact in contacts:
        inserts.append(
            f"INSERT INTO contacts (id, birth_date, effective_date, state) VALUES "
            f"({contact['id']}, '{contact['birth_date']}', '{contact['effective_date']}', "
            f"'{contact['state']}');"
        )
        
        # Add to combined contacts list
        contact_copy = contact.copy()
        contact_copy['organization_id'] = org_id
        all_contacts.append(contact_copy)
    
    # Write to a temp file
    with open(f'org_{org_id}_inserts.sql', 'w') as f:
        f.write('\n'.join(inserts))

# Write combined contacts to JSON file
with open('temp_test/contacts.json', 'w') as f:
    json.dump(all_contacts, f, indent=2)
EOF

# Create org databases using the generated SQL
for sql_file in org_*_inserts.sql; do
    if [ -f "$sql_file" ]; then
        org_id=$(echo $sql_file | cut -d'_' -f2)
        create_org_db $org_id "$(cat $sql_file)"
        rm $sql_file
    fi
done

echo -e "${BLUE}Running tests...${NC}"

# Run the optimized implementation with both sync and async modes
echo "Testing synchronous optimized version..."
./run_with_uv.sh email_scheduler_optimized.py --input temp_test/contacts.json --output temp_test/output/sync_optimized_results.json --start-date 2024-01-01

echo "Testing asynchronous optimized version..."
./run_with_uv.sh email_scheduler_optimized.py --input temp_test/contacts.json --output temp_test/output/async_optimized_results.json --start-date 2024-01-01 --async

# Run backward compatibility test (main.py - sync wrapper)
echo "Testing backward compatibility (sync)..."
./run_with_uv.sh main.py -o --start-date 2024-01-01
if [ -f "schedule_results.json" ]; then
    mv schedule_results.json temp_test/output/main_legacy_results.json
else
    echo "Error: main.py did not produce output file"
fi

# Run backward compatibility test (async_scheduler.py - async wrapper)
echo "Testing backward compatibility (async)..."
./run_with_uv.sh async_scheduler.py -o --start-date 2024-01-01
if [ -f "schedule_results.json" ]; then
    mv schedule_results.json temp_test/output/async_legacy_results.json
else
    echo "Error: async_scheduler.py did not produce output file"
fi

# Compare results
echo -e "${BLUE}Comparing results...${NC}"
python3 - <<EOF
import json
import os

def load_json(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None

# Load all result files
sync_optimized = load_json('temp_test/output/sync_optimized_results.json')
async_optimized = load_json('temp_test/output/async_optimized_results.json')
main_legacy = load_json('temp_test/output/main_legacy_results.json')
async_legacy = load_json('temp_test/output/async_legacy_results.json')

# Format and check main_legacy and async_legacy against optimized formats
def format_legacy_for_comparison(legacy_results):
    formatted = []
    for org_id, org_data in legacy_results.items():
        for contact_id, contact_data in org_data.get('scheduled_by_contact', {}).items():
            formatted.append({
                'contact_id': contact_id,
                'emails': contact_data.get('scheduled', []),
                'skipped': contact_data.get('skipped', [])
            })
    return sorted(formatted, key=lambda x: x.get('contact_id', ''))

# Check if files exist
all_exist = True
for name, data in [
    ('sync_optimized', sync_optimized),
    ('async_optimized', async_optimized),
    ('main_legacy', main_legacy),
    ('async_legacy', async_legacy)
]:
    if data is None:
        print(f"❌ {name} data not available")
        all_exist = False

if not all_exist:
    print("Cannot compare all results due to missing files")
    exit(1)

# Format legacy results
main_legacy_formatted = format_legacy_for_comparison(main_legacy)
async_legacy_formatted = format_legacy_for_comparison(async_legacy)

# Sort optimized results
sync_optimized_sorted = sorted(sync_optimized, key=lambda x: x.get('contact_id', ''))
async_optimized_sorted = sorted(async_optimized, key=lambda x: x.get('contact_id', ''))

# Compare results
print("\nComparing results:")
pairs = [
    ("sync_optimized", "async_optimized", sync_optimized_sorted, async_optimized_sorted),
    ("sync_optimized", "main_legacy", sync_optimized_sorted, main_legacy_formatted),
    ("async_optimized", "async_legacy", async_optimized_sorted, async_legacy_formatted),
    ("main_legacy", "async_legacy", main_legacy_formatted, async_legacy_formatted)
]

all_match = True
for name1, name2, data1, data2 in pairs:
    try:
        # Check length first
        if len(data1) != len(data2):
            print(f"❌ {name1} and {name2} have different numbers of results ({len(data1)} vs {len(data2)})")
            all_match = False
            continue
            
        # Compare with sorting and normalization
        str1 = json.dumps(data1, sort_keys=True)
        str2 = json.dumps(data2, sort_keys=True)
        
        if str1 == str2:
            print(f"✅ {name1} and {name2} results match exactly")
        else:
            print(f"❌ {name1} and {name2} results differ")
            all_match = False
            
            # Find a few specific differences
            print(f"First few differences (contact_id):")
            diff_count = 0
            for i, (item1, item2) in enumerate(zip(data1, data2)):
                if json.dumps(item1, sort_keys=True) != json.dumps(item2, sort_keys=True):
                    contact_id = item1.get('contact_id')
                    print(f"  - Contact {contact_id} results differ")
                    diff_count += 1
                    if diff_count >= 3:  # Show at most 3 differences
                        break
    except Exception as e:
        print(f"Error comparing {name1} and {name2}: {e}")
        all_match = False

# Final result
if all_match:
    print("\n✅ All implementations produce matching results!")
else:
    print("\n❌ Some implementations produce different results. The optimized version may not be a perfect replacement.")

EOF

# Clean up test databases
read -p "Do you want to clean up test databases? (y/n) " -n 1 -r -t 3
REPLY=${REPLY:-y}
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Cleaning up..."
    rm -rf temp_test
fi

echo -e "${GREEN}Test run completed!${NC}"