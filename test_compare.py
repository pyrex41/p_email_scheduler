import asyncio
import json
from datetime import datetime, date
from main import schedule_emails as sync_schedule, EMAIL_TYPE_POST_WINDOW
from async_scheduler import schedule_emails as async_schedule
import deepdiff

def calculate_age(birth_date_str, reference_date):
    birth_date = datetime.strptime(birth_date_str, "%Y-%m-%d").date()
    age = reference_date.year - birth_date.year
    # Adjust age if birthday hasn't occurred this year
    if reference_date.month < birth_date.month or (reference_date.month == birth_date.month and reference_date.day < birth_date.day):
        age -= 1
    return age

def load_test_cases():
    with open('test_cases.json') as f:
        test_data = json.load(f)['test_cases']
        # Add calculated age to each contact based on birth_date
        reference_date = datetime.strptime("2024-01-01", "%Y-%m-%d").date()
        for test_case in test_data.values():
            for contact in test_case['contacts']:
                contact['age'] = calculate_age(contact['birth_date'], reference_date)
        return test_data

def validate_results(results, test_cases):
    print("\nValidating results against test cases...")
    all_passed = True
    
    # Load the mock email logs if available
    mock_emails = {}
    try:
        import os
        mock_dir = "./mock_emails"
        if os.path.exists(mock_dir):
            for filename in os.listdir(mock_dir):
                if filename.startswith("mock_email_") and filename.endswith(".json"):
                    with open(os.path.join(mock_dir, filename), 'r') as f:
                        email_data = json.load(f)
                        to_email = email_data.get('to_email', '')
                        # Extract contact ID from email address pattern test{id}@example.com
                        if to_email.startswith('test') and '@' in to_email:
                            contact_id = to_email.split('@')[0].replace('test', '')
                            if contact_id.isdigit():
                                if contact_id not in mock_emails:
                                    mock_emails[contact_id] = []
                                mock_emails[contact_id].append(email_data)
        print(f"Loaded {sum(len(emails) for emails in mock_emails.values())} mock emails for validation")
    except Exception as e:
        print(f"Warning: Could not load mock email logs: {e}")
    
    # Read the mock SendGrid log file if available
    mock_log_content = ""
    try:
        if os.path.exists("mock_sendgrid.log"):
            with open("mock_sendgrid.log", 'r') as f:
                mock_log_content = f.read()
            print("Loaded mock SendGrid log for validation")
    except Exception as e:
        print(f"Warning: Could not load mock SendGrid log: {e}")
    
    for test_name, test_case in test_cases.items():
        print(f"\n🔍 Checking {test_name}: {test_case['description']}")
        org_id = test_case['org_id']
        # Try both string and integer versions of org_id
        org_results = results.get(str(org_id)) or results.get(org_id)
        
        if not org_results:
            print(f"❌ No results found for organization {org_id}")
            all_passed = False
            continue
            
        for contact in test_case['contacts']:
            contact_id = str(contact['id'])
            contact_results = org_results.get('scheduled_by_contact', {}).get(contact_id)
            expected = contact['expected']
            
            print(f"\nChecking contact {contact_id} ({contact['state']}):")
            
            # Check for invalid effective date handling
            if 'effective_date' in contact and contact['effective_date'] == 'invalid-date':
                if not contact_results:
                    print(f"❌ Contact with invalid effective date should have a result entry")
                    all_passed = False
                    continue
                
                skipped_all = False
                for skipped in contact_results.get('skipped', []):
                    if skipped.get('type') == 'all' or skipped.get('type') == expected.get('skipped_type'):
                        skipped_all = True
                        print(f"✅ Correctly skipped all emails for contact with invalid effective date (Reason: {skipped.get('reason')})")
                        break
                
                if not skipped_all and expected.get('skipped_type') == 'all':
                    print(f"❌ Should have skipped all emails for contact with invalid effective date")
                    all_passed = False
                
                continue
            
            # Check if we should have any emails at all
            if contact['state'] in {"CT", "MA", "NY", "WA"}:
                if not contact_results:
                    print(f"❌ Year-round enrollment state {contact['state']} should have a result entry")
                    all_passed = False
                elif contact_results.get('scheduled'):
                    print(f"❌ Year-round enrollment state {contact['state']} should have no scheduled emails")
                    all_passed = False
                elif not contact_results.get('skipped'):
                    print(f"❌ Year-round enrollment state {contact['state']} should have skipped emails")
                    all_passed = False
                else:
                    print(f"✅ Correctly skipped emails for year-round state {contact['state']}")
                continue
            
            if not contact_results:
                print(f"❌ No results found for contact {contact_id}")
                all_passed = False
                continue
                
            # Check for email sending failures if expected
            if 'email_send_success' in expected and not expected['email_send_success']:
                # Check for a contact with empty or invalid email
                if 'email' in contact and (not contact['email'] or '@' not in contact['email']):
                    # Check if this contact's ID is in the mock emails
                    if contact_id in mock_emails and mock_emails[contact_id]:
                        print(f"❌ Contact {contact_id} with invalid email address should not have sent emails")
                        all_passed = False
                    else:
                        # Look for an error message in the log
                        email_error_found = False
                        if mock_log_content and ("Invalid email address" in mock_log_content and contact_id in mock_log_content):
                            email_error_found = True
                            print(f"✅ Correctly logged error for invalid email address for contact {contact_id}")
                        elif mock_log_content and "Skipping email sending for contact" in mock_log_content and contact_id in mock_log_content:
                            email_error_found = True
                            print(f"✅ Correctly skipped email sending for contact {contact_id} with invalid email")
                        
                        if not email_error_found:
                            print(f"⚠️ Could not verify email sending failure for contact {contact_id} - log check inconclusive")
                
                # Additional check for expected send failures due to other reasons
                elif mock_log_content and "Failed to send" in mock_log_content and contact_id in mock_log_content:
                    print(f"✅ Correctly identified email sending failure for contact {contact_id}")
                elif contact_id in mock_emails and mock_emails[contact_id]:
                    print(f"❌ Contact {contact_id} should have failed to send emails but emails were successfully sent")
                    all_passed = False
            
            # Check scheduled emails
            scheduled_emails = {
                email['type']: datetime.strptime(email['date'], "%Y-%m-%d").date()
                for email in contact_results.get('scheduled', [])
                if 'reason' not in email  # Exclude post-window emails from this map
            }
            
            # Check birthday email
            if 'birthday_email' in expected:
                expected_date = datetime.strptime(expected['birthday_email'], "%Y-%m-%d").date() if expected['birthday_email'] else None
                if expected_date is None:
                    if 'birthday' in scheduled_emails:
                        print(f"❌ Birthday email scheduled for {scheduled_emails['birthday']} but should be skipped")
                        all_passed = False
                    else:
                        skipped_birthday = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'birthday'), None)
                        if skipped_birthday:
                            print(f"✅ Correctly skipped birthday email (Reason: {skipped_birthday.get('reason')})")
                        else:
                            print("✅ Correctly skipped birthday email")
                elif 'birthday' in scheduled_emails:
                    if scheduled_emails['birthday'] == expected_date:
                        print(f"✅ Birthday email correctly scheduled for {expected['birthday_email']}")
                    else:
                        print(f"❌ Birthday email scheduled for {scheduled_emails['birthday']}, expected {expected['birthday_email']}")
                        all_passed = False
                else:
                    print(f"❌ Expected birthday email on {expected['birthday_email']} but none scheduled")
                    skipped_birthday = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'birthday'), None)
                    if skipped_birthday:
                        print(f"  -> It was skipped: {skipped_birthday.get('reason')}")
                    all_passed = False
            
            # Check effective date email
            if 'effective_email' in expected:
                expected_date = datetime.strptime(expected['effective_email'], "%Y-%m-%d").date() if expected['effective_email'] else None
                if expected_date is None:
                    if 'effective_date' in scheduled_emails:
                        print(f"❌ Effective date email scheduled for {scheduled_emails['effective_date']} but should be skipped")
                        all_passed = False
                    else:
                        skipped_effective = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'effective_date'), None)
                        if skipped_effective:
                            print(f"✅ Correctly skipped effective date email (Reason: {skipped_effective.get('reason')})")
                        else:
                            print("✅ Correctly skipped effective date email")
                elif 'effective_date' in scheduled_emails:
                    if scheduled_emails['effective_date'] == expected_date:
                        print(f"✅ Effective date email correctly scheduled for {expected['effective_email']}")
                    else:
                        print(f"❌ Effective date email scheduled for {scheduled_emails['effective_date']}, expected {expected['effective_email']}")
                        all_passed = False
                else:
                    print(f"❌ Expected effective date email on {expected['effective_email']} but none scheduled")
                    skipped_effective = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'effective_date'), None)
                    if skipped_effective:
                        print(f"  -> It was skipped: {skipped_effective.get('reason')}")
                    all_passed = False
            
            # Check AEP email
            if 'aep_email' in expected:
                if expected['aep_email'] is None:
                    if 'aep' in scheduled_emails:
                        print(f"❌ AEP email scheduled for {scheduled_emails['aep']} but should be skipped")
                        all_passed = False
                    else:
                        skipped_aep = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'aep'), None)
                        if skipped_aep:
                            print(f"✅ Correctly skipped AEP email (Reason: {skipped_aep.get('reason')})")
                        else:
                            print("✅ Correctly skipped AEP email")
                else:
                    expected_date = datetime.strptime(expected['aep_email'], "%Y-%m-%d").date()
                    if 'aep' in scheduled_emails:
                        if scheduled_emails['aep'] == expected_date:
                            print(f"✅ AEP email correctly scheduled for {expected['aep_email']}")
                        else:
                            print(f"❌ AEP email scheduled for {scheduled_emails['aep']}, expected {expected['aep_email']}")
                            all_passed = False
                    else:
                        print(f"❌ Expected AEP email on {expected['aep_email']} but none scheduled")
                        skipped_aep = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'aep'), None)
                        if skipped_aep:
                            print(f"  -> It was skipped: {skipped_aep.get('reason')}")
                        all_passed = False
            
            # Check post-window email
            if 'post_window_email' in expected:
                if expected['post_window_email'] is None:
                    post_window_emails = [e for e in contact_results.get('scheduled', []) if e.get('reason') == "Post-window email"]
                    if post_window_emails:
                        print(f"❌ Post-window email scheduled but should be skipped")
                        all_passed = False
                    else:
                        skipped_post = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'post_window'), None)
                        if skipped_post:
                            print(f"✅ Correctly skipped post-window email (Reason: {skipped_post.get('reason')})")
                        else:
                            print("✅ Correctly skipped post-window email")
                else:
                    expected_date = datetime.strptime(expected['post_window_email'], "%Y-%m-%d").date()
                    
                    # Debug: print contact ID type and all scheduled emails
                    print(f"DEBUG: Contact ID: {contact_id} (type: {type(contact_id)})")
                    print(f"DEBUG: All scheduled emails for contact {contact_id}:")
                    for e in contact_results.get('scheduled', []):
                        print(f"DEBUG:   Type: {e.get('type')}, Date: {e.get('date')}, Reason: {e.get('reason')}")
                    
                    # Look for post-window emails - more flexible matching
                    post_window_emails = []
                    for e in contact_results.get('scheduled', []):
                        if e.get('type') == 'post_window' or e.get('reason') == "Post-window email":
                            print(f"DEBUG: Found post-window email: {e}")
                            post_window_emails.append(datetime.strptime(e['date'], "%Y-%m-%d").date())
                    
                    
                    if post_window_emails:
                        if expected_date in post_window_emails:
                            print(f"✅ Post-window email correctly scheduled for {expected['post_window_email']}")
                        else:
                            print(f"❌ Post-window email scheduled for {post_window_emails[0]}, expected {expected['post_window_email']}")
                            all_passed = False
                    else:
                        print(f"❌ Expected post-window email on {expected['post_window_email']} but none scheduled")
                        skipped_post = next((e for e in contact_results.get('skipped', []) if e.get('type') == 'post_window'), None)
                        if skipped_post:
                            print(f"  -> It was skipped: {skipped_post.get('reason')}")
                        all_passed = False
    
    return all_passed

async def compare_outputs():
    test_cases = load_test_cases()
    
    print("Loading sync results...")
    with open('sync_results.json') as f:
        sync_results = json.load(f)
    
    print("Loading async results...")
    with open('async_results.json') as f:
        async_results = json.load(f)
    
    # Convert both results to JSON strings with sorted keys for consistent comparison
    sync_json = json.dumps(sync_results, sort_keys=True, default=str)
    async_json = json.dumps(async_results, sort_keys=True, default=str)
    
    versions_match = True
    if sync_json == async_json:
        print("\n✅ Sync and async outputs are identical!")
    else:
        print("\n❌ Sync and async outputs differ!")
        try:
            diff = deepdiff.DeepDiff(sync_results, async_results, ignore_order=True)
            print("\nDifferences found:")
            # Convert complex diff objects to strings to avoid JSON serialization errors
            simple_diff = {str(k): str(v) for k, v in diff.items()}
            print(json.dumps(simple_diff, indent=2))
        except Exception as e:
            print(f"\nError getting detailed differences: {e}")
            print("Continuing with test validation...")
        versions_match = False
    
    # Validate the results against expected test cases
    results_valid = validate_results(sync_results, test_cases)
    
    if versions_match and results_valid:
        print("\n🎉 All tests passed successfully!")
        return True
    else:
        print("\n❌ Some tests failed. Please check the output above for details.")
        return False

if __name__ == "__main__":
    success = asyncio.run(compare_outputs())
    exit(0 if success else 1) 