#!/usr/bin/env python3
"""
Script to compare the email scheduling results from different implementations.
This verifies that the optimized and legacy wrappers produce equivalent results.
"""

import json
import sys
from datetime import datetime

def load_json(file_path):
    """Load JSON from a file path with error handling"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None

def format_optimized_for_comparison(optimized_results):
    """Format optimized output to match legacy format"""
    result = {}
    
    # Group by organization_id (default to "1" if not found)
    by_org = {}
    for contact_result in optimized_results:
        contact_id = contact_result.get('contact_id')
        # Try to find organization_id in the contact data if available
        org_id = "1"  # Default
        
        # Create or get the org entry
        if org_id not in by_org:
            by_org[org_id] = {
                "org_id": org_id,
                "org_name": f"Organization {org_id}",
                "scheduled_by_contact": {}
            }
            
        # Add contact results
        by_org[org_id]["scheduled_by_contact"][contact_id] = {
            "scheduled": contact_result.get('emails', []),
            "skipped": contact_result.get('skipped', [])
        }
    
    return by_org

def format_legacy_for_comparison(legacy_results):
    """Format legacy output for direct comparison"""
    # Extract just the email scheduling results
    formatted = {}
    for org_id, org_data in legacy_results.items():
        formatted[org_id] = {
            "scheduled_by_contact": {}
        }
        for contact_id, contact_data in org_data.get('scheduled_by_contact', {}).items():
            formatted[org_id]["scheduled_by_contact"][contact_id] = {
                "scheduled": contact_data.get('scheduled', []),
                "skipped": contact_data.get('skipped', [])
            }
    return formatted

def extract_emails(data):
    """Extract just the scheduled emails for simple comparison"""
    emails = []
    if isinstance(data, list):
        # Optimized format
        for contact in data:
            contact_id = contact.get('contact_id', '')
            for email in contact.get('emails', []):
                email_copy = email.copy()
                email_copy['contact_id'] = contact_id
                emails.append(email_copy)
    else:
        # Legacy format
        for org_id, org_data in data.items():
            for contact_id, contact_data in org_data.get('scheduled_by_contact', {}).items():
                for email in contact_data.get('scheduled', []):
                    email_copy = email.copy()
                    email_copy['contact_id'] = contact_id
                    emails.append(email_copy)
    
    # Sort emails for consistent comparison
    return sorted(emails, key=lambda x: (x.get('contact_id', ''), x.get('type', ''), x.get('date', '')))

def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_formats.py file1.json file2.json [file3.json]")
        sys.exit(1)
    
    file_paths = sys.argv[1:]
    data_sets = []
    
    # Load all files
    for file_path in file_paths:
        data = load_json(file_path)
        if data is not None:
            data_sets.append((file_path, data))
    
    # Extract emails from each dataset
    emails_by_file = {}
    for file_path, data in data_sets:
        emails = extract_emails(data)
        emails_by_file[file_path] = emails
    
    # Compare email sets
    all_match = True
    for i in range(len(data_sets)):
        for j in range(i+1, len(data_sets)):
            file1, _ = data_sets[i]
            file2, _ = data_sets[j]
            
            emails1 = emails_by_file[file1]
            emails2 = emails_by_file[file2]
            
            # Compare email counts
            print(f"\nComparing {file1} and {file2}:")
            print(f"- {file1}: {len(emails1)} emails")
            print(f"- {file2}: {len(emails2)} emails")
            
            if len(emails1) != len(emails2):
                print(f"❌ Email counts differ: {len(emails1)} vs {len(emails2)}")
                all_match = False
                continue
            
            # Compare email contents
            if str(emails1) == str(emails2):
                print(f"✅ Email contents match exactly")
            else:
                print(f"❌ Email contents differ")
                all_match = False
                
                # Find differences
                print("First few differences:")
                diff_count = 0
                for e1, e2 in zip(emails1, emails2):
                    if str(e1) != str(e2):
                        print(f"  Email for contact {e1.get('contact_id')}:")
                        print(f"    {file1}: {e1}")
                        print(f"    {file2}: {e2}")
                        diff_count += 1
                        if diff_count >= 3:
                            break
    
    # Final result
    if all_match:
        print("\n✅ All implementations produce matching email schedules!")
        return 0
    else:
        print("\n❌ Some implementations produce different email schedules.")
        return 1

if __name__ == "__main__":
    sys.exit(main())