import yaml
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class ContactRuleEngine:
    def __init__(self, config_file: str = 'contact_rules_config.yaml'):
        """Initialize the rule engine with configuration"""
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        self.state_rules = self.config.get('state_rules', {})
        self.timing_constants = self.config.get('timing_constants', {})
        self.aep_config = self.config.get('aep_config', {})

    def get_state_rule(self, state: str) -> Dict[str, Any]:
        """Get rules for a specific state"""
        return self.state_rules.get(state, {})

    def is_year_round_enrollment_state(self, state: str) -> bool:
        """Check if a state has year-round enrollment"""
        state_rule = self.get_state_rule(state)
        return state_rule.get('type') == 'year_round'

    def get_aep_dates(self, year: int) -> List[date]:
        """Get AEP dates for a specific year"""
        if year not in self.aep_config.get('years', []):
            return []
        
        dates = []
        for date_config in self.aep_config.get('default_dates', []):
            dates.append(date(year, date_config['month'], date_config['day']))
        return sorted(dates)

    def handle_leap_year_date(self, target_date: date, target_year: int) -> date:
        """Handle leap year date adjustments"""
        if target_date.month == 2 and target_date.day == 29:
            # If not a leap year, use February 28
            if not self._is_leap_year(target_year):
                return date(target_year, 2, 28)
        return date(target_year, target_date.month, target_date.day)

    def _is_leap_year(self, year: int) -> bool:
        """Check if a year is a leap year"""
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

    def calculate_exclusion_window(self, base_date: date, state_rule: Dict[str, Any]) -> Tuple[date, date]:
        """Calculate exclusion window based on state rule"""
        pre_window_days = self.timing_constants.get('pre_window_exclusion_days', 60)
        window_before = state_rule.get('window_before', 0)
        window_after = state_rule.get('window_after', 0)
        
        window_start = base_date - timedelta(days=pre_window_days + window_before)
        window_end = base_date + timedelta(days=window_after)
        
        return window_start, window_end

    def calculate_email_dates(self, contact: Dict[str, Any], current_date: date, end_date: date, 
                            total_contacts: int = 1, contact_index: int = 0) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate all email dates for a contact, applying state rules and exclusions.
        Returns dict with 'scheduled' and 'skipped' email lists.
        """
        result = {
            'scheduled': [],
            'skipped': []
        }

        # Skip everything for year-round enrollment states
        state = contact.get('state', '')
        if self.is_year_round_enrollment_state(state):
            return result

        state_rule = self.get_state_rule(state)
        rule_type = state_rule.get('type')

        # Calculate base dates first
        emails_to_schedule = []
        
        # 1. Birthday emails
        if contact.get('birth_date'):
            birth_date = datetime.strptime(contact['birth_date'], '%Y-%m-%d').date()
            days_before = self.timing_constants.get('birthday_email_days_before', 14)
            
            # Calculate for current and next year
            for year in range(current_date.year, end_date.year + 1):
                yearly_birth_date = self.handle_leap_year_date(birth_date, year)
                email_date = yearly_birth_date - timedelta(days=days_before)
                
                if current_date <= email_date <= end_date:
                    emails_to_schedule.append({
                        'type': 'birthday',
                        'date': email_date,
                        'base_date': yearly_birth_date
                    })

        # 2. Effective date emails
        if contact.get('effective_date'):
            eff_date = datetime.strptime(contact['effective_date'], '%Y-%m-%d').date()
            days_before = self.timing_constants.get('effective_date_days_before', 30)
            
            for year in range(current_date.year, end_date.year + 1):
                yearly_eff_date = date(year, eff_date.month, eff_date.day)
                email_date = yearly_eff_date - timedelta(days=days_before)
                
                if current_date <= email_date <= end_date:
                    emails_to_schedule.append({
                        'type': 'effective_date',
                        'date': email_date,
                        'base_date': yearly_eff_date
                    })

        # 3. AEP emails
        for year in range(current_date.year, end_date.year + 1):
            aep_dates = self.get_aep_dates(year)
            if aep_dates:
                # Distribute contacts evenly across AEP dates if batching
                if total_contacts > 1:
                    aep_index = contact_index % len(aep_dates)
                else:
                    aep_index = 0
                
                aep_date = aep_dates[aep_index]
                if current_date <= aep_date <= end_date:
                    emails_to_schedule.append({
                        'type': 'aep',
                        'date': aep_date,
                        'base_date': aep_date
                    })

        # Apply state rules and exclusions
        if rule_type in ('birthday', 'effective_date'):
            # Calculate exclusion windows
            exclusion_windows = []
            base_date_type = 'birth_date' if rule_type == 'birthday' else 'effective_date'
            
            if contact.get(base_date_type):
                base_date = datetime.strptime(contact[base_date_type], '%Y-%m-%d').date()
                
                for year in range(current_date.year, end_date.year + 1):
                    yearly_base_date = self.handle_leap_year_date(base_date, year)
                    window_start, window_end = self.calculate_exclusion_window(yearly_base_date, state_rule)
                    
                    # Only include windows that overlap with our date range
                    if window_end >= current_date and window_start <= end_date:
                        exclusion_windows.append((window_start, window_end))
                        
                        # Add post-window email
                        post_window_date = window_end + timedelta(days=1)
                        if current_date <= post_window_date <= end_date:
                            result['scheduled'].append({
                                'type': 'post_window',
                                'date': post_window_date
                            })

            # Check each email against exclusion windows
            for email in emails_to_schedule:
                email_date = email['date']
                is_excluded = False
                
                for window_start, window_end in exclusion_windows:
                    if window_start <= email_date <= window_end:
                        is_excluded = True
                        result['skipped'].append({
                            'type': email['type'],
                            'date': email_date,
                            'reason': 'In exclusion window'
                        })
                        break
                
                if not is_excluded:
                    result['scheduled'].append({
                        'type': email['type'],
                        'date': email_date
                    })
        else:
            # For states without birthday/effective_date rules, schedule all emails
            for email in emails_to_schedule:
                result['scheduled'].append({
                    'type': email['type'],
                    'date': email['date']
                })

        # Sort scheduled emails by date
        result['scheduled'].sort(key=lambda x: x['date'])

        return result

    def get_state_window_period(self, state: str) -> Dict[str, int]:
        """Get window period configuration for a state"""
        state_rules = self.get_state_rule(state)
        return {
            'window_before': state_rules.get('window_before', 0),
            'window_after': state_rules.get('window_after', 0)
        }

    def get_special_state_rules(self, state: str) -> Dict[str, Any]:
        """Get special rules for a state"""
        state_rules = self.get_state_rule(state)
        return state_rules.get('special_rules', {})

    def get_special_rule_states(self) -> List[str]:
        """Get list of states with special rules"""
        return [state for state, rule in self.state_rules.items() 
                if rule.get('type') in ('birthday', 'effective_date')] 