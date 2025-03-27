import yaml
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class ContactRuleEngine:
    def __init__(self, config_file: str = 'contact_rules_config.yaml'):
        """Initialize the rule engine with configuration"""
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        self.contact_rules = self.config.get('contact_rules', {})
        self.global_rules = self.config.get('global_rules', {})
        self.state_rules = self.config.get('state_rules', {})
        self.timing_constants = self.config.get('timing_constants', {})
        self.aep_config = self.config.get('aep_config', {})

    def get_contact_rules(self, contact_id: str) -> Dict[str, Any]:
        """Get specific rules for a contact, falling back to global rules"""
        return self.contact_rules.get(str(contact_id), {})

    def get_state_rules(self, state: str) -> Dict[str, Any]:
        """Get rules for a specific state"""
        return self.state_rules.get(state, {})

    def get_timing_constant(self, name: str, default: int) -> int:
        """Get a timing constant from config, falling back to default"""
        return self.timing_constants.get(name, default)

    def get_aep_dates(self, year: int) -> List[date]:
        """Get AEP dates for a specific year"""
        if year not in self.aep_config.get('years', []):
            return []
        
        dates = []
        for date_config in self.aep_config.get('default_dates', []):
            dates.append(date(year, date_config['month'], date_config['day']))
        return dates

    def should_force_aep_email(self, contact: Dict[str, Any]) -> bool:
        """Determine if AEP email should be forced for a contact"""
        contact_rules = self.get_contact_rules(contact['id'])
        return contact_rules.get('force_aep', False)

    def get_aep_date_override(self, contact: Dict[str, Any], current_date: date) -> Optional[date]:
        """Get AEP date override if applicable"""
        contact_rules = self.get_contact_rules(contact['id'])
        
        # Check contact-specific override
        override = contact_rules.get('aep_date_override')
        if override:
            return date(current_date.year, override['month'], override['day'])

        # Check October birthday global rule
        if contact.get('birth_date'):
            birth_date = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
            if birth_date.month == 10:
                october_rule = self.global_rules.get('october_birthday_aep')
                if october_rule:
                    return date(current_date.year, october_rule['month'], october_rule['day'])
        
        return None

    def get_post_window_dates(self, contact: Dict[str, Any], current_date: date) -> List[date]:
        """Calculate post-window dates based on rules"""
        dates = []
        contact_rules = self.get_contact_rules(contact['id'])
        
        if not contact.get('birth_date'):
            return dates

        birth_date = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
        state = contact.get('state')
        state_rules = self.get_state_rules(state)

        # Apply contact-specific post window rules
        for rule in contact_rules.get('post_window_rules', []):
            condition = rule.get('condition', {})
            if (condition.get('birth_month') == birth_date.month and
                state in condition.get('states', [])):
                override = rule.get('override_date')
                if override:
                    dates.append(date(current_date.year, override['month'], override['day']))

        # Apply state-specific rules
        if state_rules:
            # Handle leap year special case
            if birth_date.month == 2 and birth_date.day == 29:
                state_specific = self.global_rules.get('state_specific_rules', {}).get(state, {})
                leap_year_override = state_specific.get('leap_year_override')
                if leap_year_override:
                    dates.append(date(current_date.year, leap_year_override['month'], leap_year_override['day']))

        return dates

    def get_state_window_period(self, state: str) -> Dict[str, int]:
        """Get window period configuration for a state"""
        state_rules = self.get_state_rules(state)
        return {
            'window_before': state_rules.get('window_before', 0),
            'window_after': state_rules.get('window_after', 0)
        }

    def is_year_round_enrollment_state(self, state: str) -> bool:
        """Check if a state has year-round enrollment"""
        state_rules = self.get_state_rules(state)
        return state_rules.get('type') == 'year_round'

    def get_special_state_rules(self, state: str) -> Dict[str, Any]:
        """Get special rules for a state"""
        state_rules = self.get_state_rules(state)
        return state_rules.get('special_rules', {}) 