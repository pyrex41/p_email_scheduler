#!/usr/bin/env python3
"""
Script to verify that the legacy wrappers (main.py and async_scheduler.py)
produce identical results to each other.
"""

import json
import sys

def load_json(file_path):
    """Load JSON from a file path with error handling"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None

def main():
    print("Comparing main.py and async_scheduler.py wrapper outputs:")
    
    # Load the JSON files
    main_results = load_json("test_out/main.json")
    async_results = load_json("test_out/async.json")
    
    if main_results is None or async_results is None:
        print("❌ Could not load one or both output files.")
        return 1
    
    # Compare the results at a high level
    if str(main_results) == str(async_results):
        print("✅ Legacy wrappers produce identical JSON output")
        return 0
    else:
        print("❌ Legacy wrappers produce different JSON output")
        
        # Count emails
        main_email_count = sum(len(org['scheduled_by_contact'][contact]['scheduled']) 
                              for org in main_results.values()
                              for contact in org['scheduled_by_contact'])
        
        async_email_count = sum(len(org['scheduled_by_contact'][contact]['scheduled']) 
                               for org in async_results.values()
                               for contact in org['scheduled_by_contact'])
        
        print(f"- main.py: {main_email_count} emails scheduled")
        print(f"- async_scheduler.py: {async_email_count} emails scheduled")
        
        return 1

if __name__ == "__main__":
    sys.exit(main())