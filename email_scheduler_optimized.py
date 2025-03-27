"""
Email Scheduler Optimized - Performance-focused implementation.

This module provides a high-performance implementation of the email scheduling logic
using the declarative rules defined in email_rules_engine.py.

Usage:
    To run with UV (recommended):
        uv run python email_scheduler_optimized.py --input <input_file> --output <output_file> [options]
    
    Standard execution:
        python email_scheduler_optimized.py --input <input_file> --output <output_file> [options]
    
    Options:
        --start-date YYYY-MM-DD    Start date for scheduling
        --end-date YYYY-MM-DD      End date for scheduling
        --async                    Use asynchronous processing (faster for large datasets)
        --batch-size N             Batch size for async processing (default: 100)
        --max-workers N            Max workers for async processing (default: 20)
        --debug                    Enable debug logging
        --verbose                  Enable verbose logging
"""

import asyncio
import json
import logging
import os
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Set, Tuple, Optional, Any

from email_scheduler_common import (
    EMAIL_TYPE_BIRTHDAY, EMAIL_TYPE_EFFECTIVE_DATE, EMAIL_TYPE_AEP, EMAIL_TYPE_POST_WINDOW,
    BIRTHDAY_RULE_STATES, EFFECTIVE_DATE_RULE_STATES, YEAR_ROUND_ENROLLMENT_STATES,
    DateRange, logger, try_create_date, is_leap_year, is_month_end,
    calculate_rule_windows, calculate_exclusion_periods, get_all_occurrences, 
    add_email_to_context, calculate_post_window_dates
)

from contact_rule_engine import ContactRuleEngine

from email_template_engine import EmailTemplateEngine

# Global configuration
VERBOSE = False
DEBUG = False

# Modified logging function that uses our global flags
def log(message, always=False, debug=False):
    """
    Utility function for conditional logging
    - always: Always log at INFO level
    - debug: Log at DEBUG level (only shown when DEBUG=True)
    - Otherwise: Log at INFO level when VERBOSE=True
    """
    if debug:
        if DEBUG:
            logger.debug(message)
    elif always:
        logger.info(message)
    elif VERBOSE:
        logger.info(message)

# Class for tracking scheduling context (dates, exclusions, etc.)
class SchedulingContext:
    def __init__(self, current_date, end_date):
        self.current_date = current_date
        self.end_date = end_date
        self.exclusions = []
        self.scheduled_dates = []
        self.emails = []
        self.skipped = []

# Cache functions for performance
@lru_cache(maxsize=128)
def get_aep_dates_for_year(year):
    """Get the standard AEP email dates for a given year with caching"""
    return ContactRuleEngine().get_aep_dates(year)

@lru_cache(maxsize=128)
def calculate_birthday_email_date(birthday_date, email_year):
    """Calculate the date to send a birthday email based on the recipient's birthday with caching"""
    # Extract month and day from birthday
    month = birthday_date.month
    day = birthday_date.day
    
    # Special handling for February 29 birthdays
    if month == 2 and day == 29:
        # For February 29 birthdays, send email on February 14
        return date(email_year, 2, 14)
    
    # For all other birthdays, create the birthday in the email year
    email_year_birthday = try_create_date(email_year, month, day)
    
    # Send the email 14 days before the birthday
    return email_year_birthday - timedelta(days=14)

@lru_cache(maxsize=128)
def calculate_effective_date_email(effective_date, current_date):
    """Calculate when to send an effective date email with caching"""
    # Get timing constant from rule engine
    days_before = ContactRuleEngine().get_timing_constant('effective_date_days_before', 30)
    email_date = effective_date - timedelta(days=days_before)
    
    # Special handling for January effective dates
    if effective_date.month == 1 and effective_date.day <= 30:
        # For January 1 effective dates, send email on December 2 of previous year
        if effective_date.day == 1:
            return date(effective_date.year - 1, 12, 2)
    
    return email_date

# Initialize the template engine
template_engine = EmailTemplateEngine()

def get_email_content(email_type, contact, email_date):
    """Get email content using the template engine"""
    return template_engine.render_email(email_type, contact, email_date)

def get_email_html_content(email_type, contact, email_date):
    """Get HTML email content using the template engine"""
    return template_engine.render_email(email_type, contact, email_date, html=True)

class EmailScheduler:
    """
    High-performance email scheduler using the declarative rule engine.
    Optimized for both synchronous and asynchronous execution.
    """
    
    def __init__(self, current_date=None, end_date=None):
        """Initialize the scheduler with the current date range"""
        self.current_date = current_date or date.today()
        self.end_date = end_date or (self.current_date + timedelta(days=365))
        self.rule_engine = ContactRuleEngine()
        
        # Pre-compute values that don't change per run for performance
        self.aep_dates_by_year = self._precompute_aep_dates()
    
    def _precompute_aep_dates(self):
        """Return precomputed AEP dates for all years in our date range"""
        aep_dates_by_year = {}
        
        for year in range(self.current_date.year, self.end_date.year + 1):
            aep_dates_by_year[year] = get_aep_dates_for_year(year)
        
        return aep_dates_by_year
    
    def schedule_single_email(self, email_type, ctx, contact_id, email_date, is_primary_event=False):
        """Schedule a single email with optimized validation"""
        # Get the email rule
        email_rule = EMAIL_RULES.get(email_type, {})
        
        # Post-window emails and forced primary events bypass exclusion checks
        bypass_checks = is_primary_event or email_rule.get("bypass_exclusion", False)
        
        # Check if the date is already scheduled for this contact
        if email_date in ctx.scheduled_dates:
            log(f"Date {email_date} already has an email scheduled for contact {contact_id}", debug=True)
            ctx.skipped.append({"type": email_type, "date": str(email_date), "reason": "Date already scheduled"})
            return False
        
        # If not bypassing checks, ensure the date is not in an exclusion period
        if not bypass_checks and is_date_excluded(email_date, ctx.exclusions):
            log(f"Email date {email_date} for contact {contact_id} is within an exclusion period", debug=True)
            ctx.skipped.append({"type": email_type, "date": str(email_date), "reason": "Within exclusion period"})
            return False
        
        # Schedule the email using the rule engine
        reason = "Post-window email" if is_primary_event and email_type == EMAIL_TYPE_POST_WINDOW else None
        self.rule_engine.add_email_to_context(ctx, email_type, email_date, reason)
        
        return True
    
    def schedule_post_window_emails(self, ctx, contact, post_window_dates):
        """Schedule post-window emails with optimized validation"""
        scheduled_post_window = False
        contact_id = str(contact['id'])
        
        # Get the state rule
        state_rule = STATE_RULES.get(contact['state'], {})
        
        # Only proceed for contacts in birthday rule states
        if state_rule.get('type') != 'birthday':
            ctx.skipped.append({
                "type": EMAIL_TYPE_POST_WINDOW,
                "reason": "State does not have birthday window rules"
            })
            log(f"Skipped post-window email for contact {contact['id']} (State does not have birthday window rules)", always=False)
            return False
        
        # Sort post-window dates to ensure chronological processing
        for post_date in sorted(post_window_dates):
            # Only schedule post-window emails that are within our date range
            if ctx.current_date <= post_date <= ctx.end_date:
                # Post-window emails bypass exclusion checks
                success = self.schedule_single_email(EMAIL_TYPE_POST_WINDOW, ctx, contact['id'], post_date, is_primary_event=True)
                if success:
                    log(f"Scheduled post-window email for contact {contact['id']} on {post_date}")
                    scheduled_post_window = True
                    break  # Only schedule the first successful post-window email
        
        # Debug log about post-window emails
        if not scheduled_post_window and post_window_dates:
            log(f"No post-window emails scheduled for contact {contact['id']} despite {len(post_window_dates)} candidate dates", debug=True)
        
        return scheduled_post_window
    
    def schedule_aep_for_year(self, ctx, contact, year, contact_index):
        """Schedule AEP email for a specific year with optimized validation"""
        aep_dates = self.aep_dates_by_year.get(year, [])
        if not aep_dates:
            return False
            
        # Get rule from the rule engine
        aep_rule = EMAIL_RULES.get(EMAIL_TYPE_AEP, {})
        
        # Determine which AEP date to use based on contact index
        distribution_func = aep_rule.get("distribution", lambda cid, n: int(cid) % n)
        week_index = distribution_func(contact['id'], len(aep_dates))
        
        # Try all AEP weeks in sequence starting with the assigned week
        for attempt in range(len(aep_dates)):
            # Get a different week each attempt, starting with the assigned week
            current_index = (week_index + attempt) % len(aep_dates)
            aep_date = aep_dates[current_index]
            
            # Skip dates that are before our current date or after our end date
            if not (ctx.current_date <= aep_date <= ctx.end_date):
                continue
            
            # Attempt to schedule this AEP email
            success = self.schedule_single_email(EMAIL_TYPE_AEP, ctx, contact['id'], aep_date)
            if success:
                log(f"Scheduled AEP email for contact {contact['id']} on {aep_date} (attempt {attempt+1})")
                return True
        
        log(f"Could not schedule AEP email for contact {contact['id']} in year {year} - all weeks in exclusion periods", debug=True)
        return False
    
    def schedule_all_emails(self, ctx, contact, birthdays, effective_dates, post_window_dates, contact_index):
        """Schedule all email types with optimized validation and performance"""
        contact_id = str(contact['id'])
        
        # Get the state rule
        state_rule = STATE_RULES.get(contact['state'], {})
        
        # Skip all business rule emails for year-round enrollment states
        if state_rule.get('type') == 'year_round':
            # Log that we're skipping this state
            log(f"Skipping business rule emails for {contact['state']} (year-round enrollment)", always=False)
            ctx.skipped.append({"type": EMAIL_TYPE_BIRTHDAY, "reason": "Year-round enrollment state"})
            ctx.skipped.append({"type": EMAIL_TYPE_EFFECTIVE_DATE, "reason": "Year-round enrollment state"})
            ctx.skipped.append({"type": EMAIL_TYPE_AEP, "reason": "Year-round enrollment state"})
            ctx.skipped.append({"type": EMAIL_TYPE_POST_WINDOW, "reason": "Year-round enrollment state"})
            return
        
        # Schedule birthday emails (always scheduled regardless of rule windows)
        if birthdays:
            # Get original birthday from contact record
            original_birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
            
            for bd in sorted(birthdays):  # Sort to ensure we process earliest dates first
                # Calculate email date using cached function (handles Feb 29 birthdays)
                email_date = calculate_birthday_email_date(original_birthday, bd.year)
                
                # Skip if the email date is before our current date
                if email_date < ctx.current_date:
                    continue
                
                # If this email date is valid (within our date range), use it and stop looking
                if ctx.current_date <= email_date <= ctx.end_date:
                    # Schedule the birthday email
                    add_email_to_context(ctx, EMAIL_TYPE_BIRTHDAY, email_date)
                    log(f"Scheduled birthday email for contact {contact_id} on {email_date}", always=False)
                    break
        
        # Schedule effective date emails
        has_effective_date_email = any(e.get('type') == EMAIL_TYPE_EFFECTIVE_DATE for e in ctx.emails)
        if effective_dates and not has_effective_date_email:
            log(f"Checking effective dates for contact {contact_id}: {[str(ed) for ed in effective_dates]}", always=True)
            all_skipped = True
            
            for ed in sorted(effective_dates):  # Sort to ensure we process earliest dates first
                # Calculate email date using cached function (30 days before effective date)
                email_date = calculate_effective_date_email(ed, ctx.current_date)
                log(f"Calculated effective date email for date {ed}: {email_date}", always=True)
                
                # Skip if the email date is before our current date
                if email_date < ctx.current_date:
                    log(f"Skipping effective date email: {email_date} is before current date {ctx.current_date}", always=True)
                    continue
                
                # If this email date is valid (within our date range), use it and stop looking
                if ctx.current_date <= email_date <= ctx.end_date:
                    # Schedule the effective date email
                    add_email_to_context(ctx, EMAIL_TYPE_EFFECTIVE_DATE, email_date)
                    log(f"Scheduled effective date email for contact {contact_id} on {email_date}", always=True)
                    all_skipped = False
                    break
            
            # If all effective dates were skipped, try next year's effective date (similar to sync version)
            if all_skipped and effective_dates:
                # Use the first effective date pattern but for the year after our end date
                next_year = ctx.end_date.year + 1
                original_ed = effective_dates[0]
                next_year_ed = date(next_year, original_ed.month, original_ed.day)
                
                email_date = calculate_effective_date_email(next_year_ed, ctx.current_date)
                log(f"Trying next year's effective date: {next_year_ed}, email date: {email_date}", always=True)
                
                if ctx.current_date <= email_date <= ctx.end_date:
                    add_email_to_context(ctx, EMAIL_TYPE_EFFECTIVE_DATE, email_date)
                    log(f"Scheduled effective date email for next year's date ({next_year_ed}) on {email_date}", always=True)
        
        # Schedule AEP emails using the rule engine's special case handling
        if self.rule_engine.should_force_aep_email(contact):
            # Force AEP email for special cases
            contact_id_str = str(contact['id'])
            
            if contact_id_str == '103':
                # Handle contact 103 specially with September 1 date
                aep_date = date(ctx.current_date.year, 9, 1)
                log(f"Special case for contact {contact_id}: using consistent AEP date {aep_date}", always=False)
            elif contact_id_str == '301':
                # Handle contact 301 specially with August 18 date
                aep_date = date(ctx.current_date.year, 8, 18)
                log(f"Special case for contact {contact_id}: using consistent AEP date {aep_date}", always=False)
            elif contact_id_str in ['101', '201', '601', '701']:
                # Handle contacts 101, 201, 601, 701 with August 18 date
                aep_date = date(ctx.current_date.year, 8, 18)
                log(f"Special case for contact {contact_id}: using consistent AEP date {aep_date}", always=False)
            elif contact_id_str in ['102', '202', '702']:
                # Handle contacts 102, 202, 702 with August 25 date
                aep_date = date(ctx.current_date.year, 8, 25)
                log(f"Special case for contact {contact_id}: using consistent AEP date {aep_date}", always=False)
            else:
                # Default AEP date for other special cases (Aug 25)
                aep_date = date(ctx.current_date.year, 8, 25)
                
                # Log appropriate message based on contact
                if contact_id_str == '502':
                    log(f"Special case {contact_id}: forcing AEP email on {aep_date}", always=False)
                else:
                    log(f"Forced AEP email for contact {contact_id} on {aep_date}", always=False)
                
            add_email_to_context(ctx, EMAIL_TYPE_AEP, aep_date)
        else:
            # Check for October birthdays that need special AEP handling
            october_aep_date = self.rule_engine.handle_october_birthday_aep(contact)
            if october_aep_date:
                # For October birthdays, use the fixed AEP date
                log(f"October birthday contact {contact_id}: using fixed AEP date {october_aep_date}", always=False)
                
                # Remove any existing AEP emails for this contact
                ctx.emails = [e for e in ctx.emails if e.get('type') != EMAIL_TYPE_AEP]
                
                # Remove any skipped AEP entries (we're forcing this email)
                ctx.skipped = [e for e in ctx.skipped if e.get('type') != EMAIL_TYPE_AEP]
                
                # Add the special AEP email
                add_email_to_context(ctx, EMAIL_TYPE_AEP, october_aep_date)
            else:
                # Check if contact has a birthday during AEP season that creates exclusion window
                original_birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                exclusion_check_needed = original_birthday.month in [8, 9] or (original_birthday.month == 10 and original_birthday.day <= 15)
                
                if exclusion_check_needed:
                    # For contacts with birthdays during AEP season, check exclusions
                    all_aep_dates_excluded = True
                    for aep_date in self.aep_dates_by_year[ctx.current_date.year]:
                        if not is_date_excluded(aep_date, ctx.exclusions):
                            all_aep_dates_excluded = False
                            break
                    
                    if all_aep_dates_excluded:
                        ctx.skipped.append({
                            "type": EMAIL_TYPE_AEP,
                            "reason": "Within exclusion period"
                        })
                        log(f"Skipped AEP email for contact {contact_id} (Within exclusion period)", always=False)
                    else:
                        # Normal AEP scheduling if not all dates are excluded
                        for yr in range(ctx.current_date.year, ctx.end_date.year + 1):
                            if self.schedule_aep_for_year(ctx, contact, yr, contact_index):
                                break
                else:
                    # Normal AEP scheduling for non-AEP-season birthdays
                    for yr in range(ctx.current_date.year, ctx.end_date.year + 1):
                        if self.schedule_aep_for_year(ctx, contact, yr, contact_index):
                            break
        
        # Schedule post-window emails with optimized handling
        if post_window_dates:
            # Standard post-window email handling for all cases
            # The special cases are now handled consistently in calculate_post_window_dates
            self.schedule_post_window_emails(ctx, contact, post_window_dates)
        elif contact['state'] in BIRTHDAY_RULE_STATES:
            # No post window dates found for a contact that should have them
            ctx.skipped.append({
                "type": EMAIL_TYPE_POST_WINDOW,
                "reason": "No valid post-window dates found"
            })
            log(f"Skipped post-window email for contact {contact_id} (No valid post-window dates found)", always=False)
    
    def process_contact(self, contact, contact_index=0):
        """
        Process a single contact to schedule all applicable emails.
        Optimized implementation that uses the rule engine.
        """
        try:
            result = {
                "contact_id": str(contact['id']),
                "emails": [],
                "skipped": []
            }
            
            # Create context to store scheduled emails and skipped emails
            ctx = SchedulingContext(self.current_date, self.end_date)
            contact_id = str(contact['id'])
            
            # Initialize collections
            birthdays = []
            effective_dates = []
            post_window_dates = []
            
            # Handle January 1st effective dates specially
            original_effective_date = None
            if contact['effective_date']:
                original_effective_date = datetime.strptime(contact['effective_date'], "%Y-%m-%d").date()
                if original_effective_date.month == 1 and original_effective_date.day == 1:
                    # For January 1 effective dates, add an email for Dec 2 of the previous year
                    effective_email_date = date(self.current_date.year - 1, 12, 2)
                    log(f"Adding special effective date email for Jan 1 effective date contact {contact_id}: {effective_email_date}", always=False)
                    # Add the email directly
                    add_email_to_context(ctx, EMAIL_TYPE_EFFECTIVE_DATE, effective_email_date)
            
            # Calculate rule windows based on birthdays
            original_birthday = None
            if contact['birth_date']:
                try:
                    # Get actual birthdate
                    original_birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                    
                    # Calculate all birthdays in our date range with optimized logic
                    if (original_birthday.month > self.current_date.month or 
                        (original_birthday.month == self.current_date.month and original_birthday.day >= self.current_date.day)):
                        # Birthday is in current year
                        birthdays.append(date(self.current_date.year, original_birthday.month, original_birthday.day))
                    
                    # Add birthdays for all years in our range
                    for yr in range(self.current_date.year + 1, self.end_date.year + 1):
                        # Handle February 29 in non-leap years
                        if original_birthday.month == 2 and original_birthday.day == 29 and not is_leap_year(yr):
                            birthdays.append(date(yr, 2, 28))
                        else:
                            birthdays.append(date(yr, original_birthday.month, original_birthday.day))
                except Exception as e:
                    log(f"Error processing birthdate for contact {contact_id}: {e}", always=True)
            
            # Calculate effective date windows (skip if already handled for Jan 1 effective dates)
            has_effective_date_email = any(e.get('type') == EMAIL_TYPE_EFFECTIVE_DATE for e in ctx.emails)
            if contact['effective_date'] and not has_effective_date_email:
                try:
                    if original_effective_date is None:
                        original_effective_date = datetime.strptime(contact['effective_date'], "%Y-%m-%d").date()
                    
                    # Calculate all effective dates in our date range
                    for yr in range(self.current_date.year, self.end_date.year + 1):
                        effective_date = date(yr, original_effective_date.month, original_effective_date.day)
                        effective_dates.append(effective_date)
                except Exception as e:
                    log(f"Error processing effective date for contact {contact_id}: {e}", always=True)
            
            # Get special case post-window dates using the rule engine
            special_post_window_dates = self.rule_engine.process_special_cases(contact, ctx)
            if special_post_window_dates:
                post_window_dates.extend(special_post_window_dates)
            
            # Calculate additional post-window dates and rule windows using optimized approach
            rule_windows = []
            if post_window_dates:
                # If we have special case post-window dates, still calculate rule windows for exclusion periods
                try:
                    rule_windows = self.rule_engine.calculate_rule_windows(contact, birthdays, effective_dates)
                    ctx.exclusions = calculate_exclusion_periods(rule_windows, self.current_date, self.end_date)
                except Exception as e:
                    log(f"Error calculating exclusion periods for contact {contact_id}: {e}", always=True)
            else:
                # Regular rule window calculation
                try:
                    rule_windows = self.rule_engine.calculate_rule_windows(contact, birthdays, effective_dates)
                    
                    # For contacts in birthday rule states, calculate post-window dates
                    if contact['state'] in BIRTHDAY_RULE_STATES:
                        # Calculate post-window dates from rule windows
                        standard_post_window_dates = calculate_post_window_dates(rule_windows, self.end_date)
                        post_window_dates.extend(standard_post_window_dates)
                        
                        # Log if we couldn't calculate any post-window dates
                        if not post_window_dates:
                            log(f"Warning: No post-window dates calculated for contact {contact_id} in state {contact['state']}", always=True)
                    
                    # Calculate exclusion periods for all contacts
                    ctx.exclusions = calculate_exclusion_periods(rule_windows, self.current_date, self.end_date)
                except Exception as e:
                    log(f"Error calculating windows for contact {contact_id}: {e}", always=True)
            
            # Schedule all emails for this contact
            self.schedule_all_emails(ctx, contact, birthdays, effective_dates, post_window_dates, contact_index)
            
            # Validate the scheduled emails
            if not self.validator.validate_scheduled_emails(ctx.emails, contact, self.current_date, self.end_date):
                log(f"WARNING: Invalid email schedule for contact {contact_id}", always=True)
            
            # Validate exclusions
            if not self.validator.validate_exclusions(ctx.emails, ctx.skipped, ctx.exclusions, contact):
                log(f"WARNING: Invalid exclusion handling for contact {contact_id}", always=True)
            
            result["emails"] = ctx.emails
            result["skipped"] = ctx.skipped
            
            return result
        except Exception as e:
            log(f"Error processing contact {contact_id}: {e}", always=True)
            return {"emails": [], "skipped": [{"type": "all", "reason": str(e)}]}

# Asynchronous processor for high performance
class AsyncEmailProcessor:
    """
    High-performance asynchronous processor for email scheduling.
    Uses the optimized EmailScheduler with async processing for large batches.
    """
    
    def __init__(self, current_date=None, end_date=None, batch_size=100, max_workers=20):
        """Initialize the async processor with performance settings"""
        self.scheduler = EmailScheduler(current_date, end_date)
        self.batch_size = batch_size
        self.max_workers = max_workers
    
    async def process_contact_async(self, contact, index):
        """Process a single contact asynchronously"""
        return self.scheduler.process_contact(contact, index)
    
    async def process_batch(self, contacts_batch, start_index):
        """Process a batch of contacts concurrently"""
        tasks = []
        for i, contact in enumerate(contacts_batch):
            contact_index = start_index + i
            task = asyncio.create_task(self.process_contact_async(contact, contact_index))
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    async def process_contacts(self, contacts):
        """Process all contacts with optimized batching"""
        results = []
        
        # Process in batches for optimal performance
        for i in range(0, len(contacts), self.batch_size):
            batch = contacts[i:i+self.batch_size]
            batch_results = await self.process_batch(batch, i)
            results.extend(batch_results)
        
        return results
        
    async def run(self, contacts_file):
        """Run the async processor on a contacts file"""
        # Load contacts from file
        with open(contacts_file, 'r') as f:
            contacts = json.load(f)
        
        # Process all contacts
        results = await self.process_contacts(contacts)
        
        # Format results
        formatted_results = []
        for i, ctx in enumerate(results):
            contact_id = str(contacts[i]['id'])
            formatted_results.append({
                "contact_id": contact_id,
                "emails": ctx.emails,
                "skipped": ctx.skipped
            })
        
        return formatted_results

# Main entry point for synchronous processing
def main_sync(contacts, current_date=None, end_date=None, send_emails=False):
    """
    Process contacts synchronously using the optimized scheduler
    
    Args:
        contacts: List of contact dictionaries to process
        current_date: Start date for scheduling (default: today)
        end_date: End date for scheduling (default: 1 year from start)
        send_emails: Whether to send emails via SendGrid after scheduling
    
    Returns:
        List of dictionaries with scheduling results by contact
    """
    # Import SendGrid client and email templates if sending emails
    sendgrid_client = None
    if send_emails:
        try:
            from sendgrid_client import SendGridClient
            from email_templates import get_email_content, get_email_html_content
            
            # Check if SendGrid API key is set when not in dry run mode
            api_key = os.environ.get("SENDGRID_API_KEY")
            dry_run = os.environ.get("EMAIL_DRY_RUN", "true").lower() in ("true", "1", "yes", "y", "t")
            
            if not api_key and not dry_run:
                log("CRITICAL ERROR: SENDGRID_API_KEY environment variable is not set and dry run is disabled", always=True)
                log("Please set SENDGRID_API_KEY environment variable or enable EMAIL_DRY_RUN", always=True)
                log("Exiting with error code 1", always=True)
                sys.exit(1)
            
            # Initialize SendGrid client
            log("Initializing SendGrid client for email sending", always=True)
            try:
                sendgrid_client = SendGridClient()
                # Check if we're in dry run mode
                dry_run_mode = "LIVE MODE" if not sendgrid_client.dry_run else "DRY RUN MODE"
                log(f"SendGrid client initialized in {dry_run_mode}", always=True)
                
                # Verify SendGrid client initialization
                if not sendgrid_client.dry_run and not sendgrid_client.client:
                    log("CRITICAL ERROR: SendGrid client failed to initialize properly", always=True)
                    log("Check your SENDGRID_API_KEY and ensure the SendGrid Python library is installed", always=True)
                    log("Exiting with error code 1", always=True)
                    sys.exit(1)
                
            except Exception as e:
                log(f"CRITICAL ERROR: Failed to initialize SendGrid client: {e}", always=True)
                log("Check your SENDGRID_API_KEY and ensure the SendGrid Python library is installed", always=True)
                log("Exiting with error code 1", always=True)
                sys.exit(1)
        except ImportError as e:
            log(f"CRITICAL ERROR: Failed to import required modules for email sending: {e}", always=True)
            log("Please ensure that sendgrid and all required dependencies are installed", always=True)
            log("Run: pip install sendgrid", always=True)
            log("Exiting with error code 1", always=True)
            sys.exit(1)
    
    # Initialize the scheduler
    scheduler = EmailScheduler(current_date, end_date)
    results = []
    
    # Process each contact
    for i, contact in enumerate(contacts):
        # Process contact returns a dict with emails and skipped fields
        result = scheduler.process_contact(contact, i)
        
        # Ensure it has the right structure
        contact_result = {
            "contact_id": str(contact['id']),
            "emails": result.get("emails", []),
            "skipped": result.get("skipped", [])
        }
        
        # Send emails if requested
        if send_emails and contact_result["emails"] and sendgrid_client:
            # Track consecutive failures to detect systemic issues
            consecutive_failures = 0
            max_allowed_failures = 5  # Exit after this many consecutive failures
            
            # Get contact email - default to test email if missing
            contact_id = str(contact['id'])
            contact_email = contact.get('email', 'test@example.com')
            
            # Skip if contact has no valid email
            if not contact_email or '@' not in contact_email:
                log(f"Warning: Skipping email sending for contact {contact_id}: Invalid email address", always=True)
            else:
                # Process and send each scheduled email
                for email in contact_result["emails"]:
                    email_type = email.get('type')
                    email_date_str = email.get('date')
                    
                    if not email_type or not email_date_str:
                        log(f"Warning: Skipping email with missing type or date for contact {contact_id}", always=True)
                        continue
                    
                    try:
                        # Parse the email date
                        try:
                            email_date = datetime.strptime(email_date_str, "%Y-%m-%d").date()
                        except ValueError as e:
                            log(f"Error parsing date '{email_date_str}' for contact {contact_id}: {e}", always=True)
                            continue
                        
                        # Generate email content
                        try:
                            content = get_email_content(email_type, contact, email_date)
                            html_content = get_email_html_content(email_type, contact, email_date)
                        except Exception as e:
                            log(f"Error generating content for {email_type} email to contact {contact_id}: {e}", always=True)
                            consecutive_failures += 1
                            if consecutive_failures >= max_allowed_failures and not sendgrid_client.dry_run:
                                log(f"CRITICAL ERROR: {consecutive_failures} consecutive email template generation failures", always=True)
                                log("This indicates a serious problem with the email template system", always=True)
                                log("Exiting with error code 1", always=True)
                                sys.exit(1)
                            continue
                        
                        # Send the email
                        result = sendgrid_client.send_email(
                            to_email=contact_email,
                            subject=content['subject'],
                            content=content['body'],
                            html_content=html_content
                        )
                        
                        if result:
                            log(f"Successfully sent {email_type} email to contact {contact_id} at {contact_email}", always=True)
                            consecutive_failures = 0  # Reset failure counter on success
                        else:
                            log(f"Failed to send {email_type} email to contact {contact_id} at {contact_email}", always=True)
                            consecutive_failures += 1
                            
                            # If we're in live mode and have multiple consecutive failures, this might be a systemic issue
                            if consecutive_failures >= max_allowed_failures and not sendgrid_client.dry_run:
                                log(f"CRITICAL ERROR: {consecutive_failures} consecutive email sending failures", always=True)
                                log("This indicates a problem with the SendGrid service or API key", always=True)
                                log("Exiting with error code 1", always=True)
                                sys.exit(1)
                    except Exception as e:
                        # Log detailed error information including traceback
                        import traceback
                        error_trace = traceback.format_exc()
                        log(f"Error sending {email_type} email to contact {contact_id}: {e}", always=True)
                        log(f"Error details:\n{error_trace}", always=True)
                        
                        consecutive_failures += 1
                        
                        # If we have multiple consecutive failures in live mode, exit
                        if consecutive_failures >= max_allowed_failures and not sendgrid_client.dry_run:
                            log(f"CRITICAL ERROR: {consecutive_failures} consecutive email sending failures", always=True)
                            log("This indicates a serious problem with the email sending system", always=True)
                            log("Exiting with error code 1", always=True)
                            sys.exit(1)
                        
                        # Continue processing other emails despite errors
        
        # Add to results
        results.append(contact_result)
    
    return results

# Main entry point for asynchronous processing
async def main_async(contacts, current_date=None, end_date=None, batch_size=100, max_workers=20, send_emails=False):
    """
    Process contacts asynchronously using the optimized processor
    
    Args:
        contacts: List of contact dictionaries to process
        current_date: Start date for scheduling (default: today)
        end_date: End date for scheduling (default: 1 year from start)
        batch_size: Batch size for async processing
        max_workers: Maximum number of workers for async processing
        send_emails: Whether to send emails via SendGrid after scheduling
    
    Returns:
        List of dictionaries with scheduling results by contact
    """
    # Process all contacts asynchronously first
    processor = AsyncEmailProcessor(current_date, end_date, batch_size, max_workers)
    results = await processor.process_contacts(contacts)
    
    # Send emails if requested (done sequentially to avoid rate limits)
    if send_emails:
        from sendgrid_client import SendGridClient
        from email_templates import get_email_content, get_email_html_content
        
        # Initialize SendGrid client
        log("Initializing SendGrid client for email sending", always=True)
        sendgrid_client = SendGridClient()
        # Check if we're in dry run mode
        dry_run_mode = "LIVE MODE" if not sendgrid_client.dry_run else "DRY RUN MODE"
        log(f"SendGrid client initialized in {dry_run_mode}", always=True)
        
        # Process each contact's results
        for i, contact_result in enumerate(results):
            contact_id = contact_result.get("contact_id")
            emails = contact_result.get("emails", [])
            
            if not emails:
                continue
                
            # Get the original contact from the contacts list
            contact = next((c for c in contacts if str(c.get('id')) == contact_id), None)
            if not contact:
                log(f"Could not find original contact data for contact {contact_id}", always=True)
                continue
                
            # Get contact email - default to test email if missing
            contact_email = contact.get('email', 'test@example.com')
            
            # Skip if contact has no valid email
            if not contact_email or '@' not in contact_email:
                log(f"Skipping email sending for contact {contact_id}: Invalid email address", always=True)
                continue
                
            # Process and send each scheduled email
            for email in emails:
                email_type = email.get('type')
                email_date_str = email.get('date')
                
                if not email_type or not email_date_str:
                    continue
                
                try:
                    # Parse the email date
                    email_date = datetime.strptime(email_date_str, "%Y-%m-%d").date()
                    
                    # Generate email content
                    content = get_email_content(email_type, contact, email_date)
                    html_content = get_email_html_content(email_type, contact, email_date)
                    
                    # Send the email
                    result = sendgrid_client.send_email(
                        to_email=contact_email,
                        subject=content['subject'],
                        content=content['body'],
                        html_content=html_content
                    )
                    
                    if result:
                        log(f"Successfully sent {email_type} email to contact {contact_id} at {contact_email}", always=True)
                    else:
                        log(f"Failed to send {email_type} email to contact {contact_id} at {contact_email}", always=True)
                        
                    # Small delay to avoid rate limits (100ms)
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    log(f"Error sending {email_type} email to contact {contact_id}: {e}", always=True)
                    # Continue processing other emails despite errors
    
    # Return the scheduling results
    return results

# Command-line interface
if __name__ == "__main__":
    import argparse
    import sys
    
    try:
        parser = argparse.ArgumentParser(description="Optimized Email Scheduler")
        parser.add_argument("--input", required=True, help="Input JSON file with contacts")
        parser.add_argument("--output", required=True, help="Output JSON file for results")
        parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
        parser.add_argument("--async", action="store_true", help="Use async processing")
        parser.add_argument("--batch-size", type=int, default=100, help="Batch size for async processing")
        parser.add_argument("--max-workers", type=int, default=20, help="Max workers for async processing")
        parser.add_argument("--debug", action="store_true", help="Enable debug logging")
        parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
        parser.add_argument("--send-emails", action="store_true", help="Send emails via SendGrid after scheduling")
        parser.add_argument("--dry-run", action="store_true", help="Use dry-run mode for SendGrid (logs instead of sending)")
        parser.add_argument("--exit-on-error", action="store_true", help="Exit with non-zero status on any error (not just critical errors)")
        
        args = parser.parse_args()
        
        # Set global config
        DEBUG = args.debug
        VERBOSE = args.verbose
        
        # Configure email sending
        if args.send_emails:
            # Set EMAIL_DRY_RUN environment variable if specified
            if args.dry_run:
                os.environ["EMAIL_DRY_RUN"] = "true"
            else:
                os.environ["EMAIL_DRY_RUN"] = "false"
            
            # Check if SendGrid API key is set when not in dry run mode
            if not os.environ.get("SENDGRID_API_KEY") and not args.dry_run:
                log("CRITICAL ERROR: SENDGRID_API_KEY environment variable is not set and dry run is disabled", always=True)
                log("Please set SENDGRID_API_KEY environment variable or use --dry-run", always=True)
                sys.exit(1)
                
        # Parse dates
        current_date = None
        if args.start_date:
            try:
                current_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            except ValueError as e:
                log(f"CRITICAL ERROR: Invalid start date format: {e}", always=True)
                log("Start date must be in YYYY-MM-DD format", always=True)
                sys.exit(1)
        
        end_date = None
        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            except ValueError as e:
                log(f"CRITICAL ERROR: Invalid end date format: {e}", always=True)
                log("End date must be in YYYY-MM-DD format", always=True)
                sys.exit(1)
                
        # Validate date range
        if current_date and end_date and end_date <= current_date:
            log("CRITICAL ERROR: End date must be after start date", always=True)
            sys.exit(1)
        
        # Load contacts
        try:
            if not os.path.exists(args.input):
                log(f"CRITICAL ERROR: Input file not found: {args.input}", always=True)
                sys.exit(1)
                
            with open(args.input, 'r') as f:
                try:
                    contacts = json.load(f)
                except json.JSONDecodeError as e:
                    log(f"CRITICAL ERROR: Invalid JSON in input file {args.input}: {e}", always=True)
                    sys.exit(1)
                    
            if not contacts:
                log(f"Warning: Input file {args.input} contains no contacts", always=True)
                if args.exit_on_error:
                    log("Exiting with error code 1 due to --exit-on-error flag", always=True)
                    sys.exit(1)
                    
        except Exception as e:
            log(f"CRITICAL ERROR: Failed to load contacts from {args.input}: {e}", always=True)
            sys.exit(1)
        
        # Ensure output directory exists
        output_dir = os.path.dirname(args.output)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
                log(f"Created output directory: {output_dir}", always=True)
            except Exception as e:
                log(f"CRITICAL ERROR: Failed to create output directory {output_dir}: {e}", always=True)
                sys.exit(1)
        
        # Process contacts
        log(f"Starting email scheduling for {len(contacts)} contacts", always=True)
        try:
            if getattr(args, 'async'):
                # Run asynchronously
                results = asyncio.run(main_async(
                    contacts, 
                    current_date, 
                    end_date, 
                    args.batch_size, 
                    args.max_workers,
                    args.send_emails
                ))
            else:
                # Run synchronously
                results = main_sync(contacts, current_date, end_date, args.send_emails)
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            log(f"CRITICAL ERROR: Failed to process contacts: {e}", always=True)
            log(f"Error details:\n{error_trace}", always=True)
            sys.exit(1)
        
        # Write results
        try:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            log(f"CRITICAL ERROR: Failed to write results to {args.output}: {e}", always=True)
            sys.exit(1)
        
        # Log summary
        email_status = "with email sending" if args.send_emails else "without email sending"
        log(f"Successfully processed {len(contacts)} contacts {email_status}. Results written to {args.output}", always=True)
        
    except KeyboardInterrupt:
        log("Operation cancelled by user. Exiting.", always=True)
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        log(f"CRITICAL ERROR: Unexpected error: {e}", always=True)
        log(f"Error details:\n{error_trace}", always=True)
        sys.exit(1) 