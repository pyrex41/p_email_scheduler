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
        try:
            # Use the rule engine's calculate_email_dates method which handles all email types
            result = self.rule_engine.calculate_email_dates(
                contact=contact,
                current_date=current_date,
                end_date=end_date
            )
            
            # Add contact ID to result
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
        total_contacts = len(contacts)
        
        # Process in batches
        for i in range(0, len(contacts), self.batch_size):
            batch = contacts[i:i + self.batch_size]
            
            # Create tasks for batch
            tasks = [
                asyncio.create_task(self._process_contact(contact, current_date, end_date, total_contacts, idx))
                for idx, contact in enumerate(batch)
            ]
            
            # Wait for batch to complete
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            
            logger.info(f"Processed batch of {len(batch)} contacts ({i + len(batch)}/{len(contacts)})")
            
        return results
        
    async def _process_contact(self, contact: Dict[str, Any], current_date: date, end_date: date, 
                             total_contacts: int, contact_index: int) -> Dict[str, Any]:
        """Process a single contact asynchronously"""
        try:
            # Use the rule engine's calculate_email_dates method with contact distribution info
            result = self.scheduler.rule_engine.calculate_email_dates(
                contact=contact,
                current_date=current_date,
                end_date=end_date,
                total_contacts=total_contacts,
                contact_index=contact_index
            )
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
    total_contacts = len(contacts)
    
    for i, contact in enumerate(contacts):
        try:
            result = scheduler.rule_engine.calculate_email_dates(
                contact=contact,
                current_date=current_date,
                end_date=end_date,
                total_contacts=total_contacts,
                contact_index=i
            )
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