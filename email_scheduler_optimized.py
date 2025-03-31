"""
Email Scheduler Optimized - Performance-focused implementation.
Handles scheduling of emails based on contact rules and dates.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional
from contact_rule_engine import ContactRuleEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailScheduler:
    """Processes contacts to schedule emails using rule engine"""
    
    def __init__(self):
        self.rule_engine = ContactRuleEngine()
        
    def process_contact(self, contact: Dict[str, Any], current_date: date, end_date: date) -> Dict[str, Any]:
        """
        Process a single contact to determine email schedule
        
        Args:
            contact: Contact data dictionary
            current_date: Start date for scheduling
            end_date: End date for scheduling
            
        Returns:
            Dictionary containing scheduled and skipped emails
        """
        result = {
            "scheduled": [],
            "skipped": []
        }
        
        state = contact.get('state', 'CA')
        
        # Get state rules
        state_rule = self.rule_engine.get_state_rule(state)
        rule_type = state_rule.get('type') if state_rule else None
        
        # Skip all emails for year-round enrollment states
        if self.rule_engine.is_year_round_enrollment_state(state):
            result["skipped"].append({
                "type": "all",
                "reason": "Year-round enrollment state"
            })
            return result
            
        # Process birthday emails
        if rule_type == 'birthday' and contact.get('birth_date'):
            try:
                # Get actual birthdate
                if isinstance(contact['birth_date'], str):
                    birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                elif isinstance(contact['birth_date'], date):
                    birthday = contact['birth_date']
                else:
                    logger.warning(f"Invalid birth_date format for contact {contact['id']}: {contact['birth_date']}")
                    result["skipped"].append({
                        "type": "birthday",
                        "reason": "Invalid birth_date format"
                    })
                    birthday = None
                
                if birthday:
                    # Calculate email dates
                    email_dates = self.rule_engine.calculate_birthday_email_dates(
                        state, birthday, current_date, end_date
                    )
                    
                    # Add non-excluded dates to schedule
                    for email_date in email_dates:
                        if not self.rule_engine.is_date_excluded(email_date, state):
                            result["scheduled"].append({
                                "type": "birthday",
                                "date": email_date
                            })
                        else:
                            result["skipped"].append({
                                "type": "birthday",
                                "reason": "Date in exclusion window",
                                "date": email_date
                            })
                            
            except Exception as e:
                logger.error(f"Error processing birthday email for contact {contact['id']}: {e}")
                result["skipped"].append({
                    "type": "birthday",
                    "reason": str(e)
                })
                
        # Process effective date emails
        if rule_type == 'effective_date' and contact.get('effective_date'):
            try:
                # Get actual effective date
                if isinstance(contact['effective_date'], str):
                    effective_date = datetime.strptime(contact['effective_date'], "%Y-%m-%d").date()
                elif isinstance(contact['effective_date'], date):
                    effective_date = contact['effective_date']
                else:
                    logger.warning(f"Invalid effective_date format for contact {contact['id']}: {contact['effective_date']}")
                    result["skipped"].append({
                        "type": "effective_date",
                        "reason": "Invalid effective_date format"
                    })
                    effective_date = None
                    
                if effective_date:
                    # Calculate email dates
                    email_dates = self.rule_engine.calculate_effective_date_email_dates(
                        state, effective_date, current_date, end_date
                    )
                    
                    # Add non-excluded dates to schedule
                    for email_date in email_dates:
                        if not self.rule_engine.is_date_excluded(email_date, state):
                            result["scheduled"].append({
                                "type": "effective_date",
                                "date": email_date
                            })
                        else:
                            result["skipped"].append({
                                "type": "effective_date",
                                "reason": "Date in exclusion window",
                                "date": email_date
                            })
                            
            except Exception as e:
                logger.error(f"Error processing effective date email for contact {contact['id']}: {e}")
                result["skipped"].append({
                    "type": "effective_date",
                    "reason": str(e)
                })
                
        # Process AEP emails if not in year-round enrollment state
        if not self.rule_engine.is_year_round_enrollment_state(state):
            try:
                # Get AEP dates
                aep_dates = self.rule_engine.get_aep_dates_for_year(current_date.year, end_date.year)
                
                # Filter to dates in range and not excluded
                for aep_date in aep_dates:
                    if current_date <= aep_date <= end_date:
                        if not self.rule_engine.is_date_excluded(aep_date, state):
                            result["scheduled"].append({
                                "type": "aep",
                                "date": aep_date
                            })
                        else:
                            result["skipped"].append({
                                "type": "aep",
                                "reason": "Date in exclusion window",
                                "date": aep_date
                            })
                            
            except Exception as e:
                logger.error(f"Error processing AEP emails for contact {contact['id']}: {e}")
                result["skipped"].append({
                    "type": "aep",
                    "reason": str(e)
                })
                
        # Process post-window emails
        try:
            post_window_dates = self.rule_engine.calculate_post_window_dates(state, current_date, end_date)
            
            for post_date in post_window_dates:
                if not self.rule_engine.is_date_excluded(post_date, state):
                    result["scheduled"].append({
                        "type": "post_window",
                        "date": post_date
                    })
                else:
                    result["skipped"].append({
                        "type": "post_window",
                        "reason": "Date in exclusion window",
                        "date": post_date
                    })
                    
        except Exception as e:
            logger.error(f"Error processing post-window emails for contact {contact['id']}: {e}")
            result["skipped"].append({
                "type": "post_window",
                "reason": str(e)
            })
            
        return result

class AsyncEmailProcessor:
    """Allows for asynchronous processing of contacts in batches"""
    
    def __init__(self, scheduler: EmailScheduler, batch_size: int = 100):
        self.scheduler = scheduler
        self.batch_size = batch_size
        
    async def process_contacts(self, contacts: List[Dict[str, Any]], current_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Process contacts asynchronously in batches
        
        Args:
            contacts: List of contacts to process
            current_date: Start date for scheduling
            end_date: End date for scheduling
            
        Returns:
            List of results for each contact
        """
        results = []
        
        # Process in batches
        for i in range(0, len(contacts), self.batch_size):
            batch = contacts[i:i + self.batch_size]
            
            # Create tasks for batch
            tasks = [
                asyncio.create_task(self._process_contact(contact, current_date, end_date))
                for contact in batch
            ]
            
            # Wait for batch to complete
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            
            logger.info(f"Processed batch of {len(batch)} contacts ({i + len(batch)}/{len(contacts)})")
            
        return results
        
    async def _process_contact(self, contact: Dict[str, Any], current_date: date, end_date: date) -> Dict[str, Any]:
        """Process a single contact asynchronously"""
        try:
            result = self.scheduler.process_contact(contact, current_date, end_date)
            result['contact_id'] = contact.get('id')
            return result
        except Exception as e:
            logger.error(f"Error processing contact {contact.get('id')}: {e}")
            return {
                "contact_id": contact.get('id'),
                "scheduled": [],
                "skipped": [{
                    "type": "all",
                    "reason": f"Processing error: {str(e)}"
                }]
            }

async def main_async(contacts: List[Dict[str, Any]], current_date: Optional[date] = None, 
                    end_date: Optional[date] = None, batch_size: int = 100) -> List[Dict[str, Any]]:
    """
    Main async entry point for email scheduling
    
    Args:
        contacts: List of contacts to process
        current_date: Optional start date (defaults to today)
        end_date: Optional end date (defaults to 2 years from start)
        batch_size: Number of contacts to process in parallel
        
    Returns:
        List of results for each contact
    """
    # Set default dates if not provided
    if not current_date:
        current_date = date.today()
    if not end_date:
        end_date = current_date + timedelta(days=365 * 2)
        
    processor = AsyncEmailProcessor(EmailScheduler(), batch_size)
    return await processor.process_contacts(contacts, current_date, end_date)

def main_sync(contacts: List[Dict[str, Any]], current_date: Optional[date] = None,
              end_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """
    Main synchronous entry point for email scheduling
    
    Args:
        contacts: List of contacts to process
        current_date: Optional start date (defaults to today)
        end_date: Optional end date (defaults to 2 years from start)
        
    Returns:
        List of results for each contact
    """
    # Set default dates if not provided
    if not current_date:
        current_date = date.today()
    if not end_date:
        end_date = current_date + timedelta(days=365 * 2)
        
    scheduler = EmailScheduler()
    results = []
    
    for i, contact in enumerate(contacts):
        try:
            result = scheduler.process_contact(contact, current_date, end_date)
            result['contact_id'] = contact.get('id')
            results.append(result)
            
            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(contacts)} contacts")
                
        except Exception as e:
            logger.error(f"Error processing contact {contact.get('id')}: {e}")
            results.append({
                "contact_id": contact.get('id'),
                "scheduled": [],
                "skipped": [{
                    "type": "all",
                    "reason": f"Processing error: {str(e)}"
                }]
            })
            
    return results 