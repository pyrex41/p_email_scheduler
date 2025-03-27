from typing import Dict, Any, List
from datetime import date
import yaml

class ConfigValidationError(Exception):
    """Custom exception for configuration validation errors"""
    pass

class RuleConfigValidator:
    @staticmethod
    def validate_date_override(override: Dict[str, int], context: str) -> None:
        """Validate a date override configuration"""
        if not isinstance(override, dict):
            raise ConfigValidationError(f"{context}: Date override must be a dictionary")
        
        required_fields = ['month', 'day']
        for field in required_fields:
            if field not in override:
                raise ConfigValidationError(f"{context}: Missing required field '{field}'")
            
            if not isinstance(override[field], int):
                raise ConfigValidationError(f"{context}: Field '{field}' must be an integer")
        
        month = override['month']
        day = override['day']
        
        if not 1 <= month <= 12:
            raise ConfigValidationError(f"{context}: Month must be between 1 and 12")
            
        # Basic day validation (not accounting for specific months)
        if not 1 <= day <= 31:
            raise ConfigValidationError(f"{context}: Day must be between 1 and 31")
            
        # Validate the date is valid for the month
        try:
            date(2024, month, day)  # Use leap year to allow Feb 29
        except ValueError as e:
            raise ConfigValidationError(f"{context}: Invalid date - {str(e)}")

    @staticmethod
    def validate_timing_constants(config: Dict[str, Any], partial: bool = False) -> None:
        """Validate timing constants configuration"""
        if not partial and 'timing_constants' not in config:
            raise ConfigValidationError("Missing timing_constants section")
            
        constants = config.get('timing_constants', {})
        required_constants = [
            'birthday_email_days_before',
            'effective_date_days_before',
            'pre_window_exclusion_days'
        ]
        
        for constant in required_constants:
            if constant in constants:
                value = constants[constant]
                if not isinstance(value, int) or value < 0:
                    raise ConfigValidationError(f"Timing constant {constant} must be a non-negative integer")

    @staticmethod
    def validate_aep_config(config: Dict[str, Any], partial: bool = False) -> None:
        """Validate AEP configuration"""
        if not partial and 'aep_config' not in config:
            raise ConfigValidationError("Missing aep_config section")
            
        aep_config = config.get('aep_config', {})
        if aep_config:
            if 'default_dates' in aep_config:
                dates = aep_config['default_dates']
                if not isinstance(dates, list) or not dates:
                    raise ConfigValidationError("AEP default_dates must be a non-empty list")
                
                for i, date_override in enumerate(dates):
                    RuleConfigValidator.validate_date_override(
                        date_override,
                        f"AEP default date at index {i}"
                    )
            
            if 'years' in aep_config:
                years = aep_config['years']
                if not isinstance(years, list) or not years:
                    raise ConfigValidationError("AEP years must be a non-empty list")
                
                for year in years:
                    if not isinstance(year, int) or year < 2000:
                        raise ConfigValidationError(f"Invalid year in AEP config: {year}")

    @staticmethod
    def validate_state_rules(config: Dict[str, Any], partial: bool = False) -> None:
        """Validate state rules configuration"""
        if not partial and 'state_rules' not in config:
            raise ConfigValidationError("Missing state rules configuration")
            
        state_rules = config.get('state_rules', {})
        if state_rules:
            valid_types = {'birthday', 'effective_date', 'year_round'}
            
            for state, rules in state_rules.items():
                if not isinstance(rules, dict):
                    raise ConfigValidationError(f"Rules for state {state} must be a dictionary")
                    
                if 'type' not in rules:
                    raise ConfigValidationError(f"Missing rule type for state {state}")
                    
                rule_type = rules['type']
                if rule_type not in valid_types:
                    raise ConfigValidationError(f"Invalid rule type '{rule_type}' for state {state}")
                    
                if rule_type != 'year_round':
                    if 'window_before' not in rules or 'window_after' not in rules:
                        raise ConfigValidationError(f"State {state} missing window_before or window_after")
                        
                    if not isinstance(rules['window_before'], int) or not isinstance(rules['window_after'], int):
                        raise ConfigValidationError(f"Window periods for state {state} must be integers")

    @staticmethod
    def validate_contact_rules(config: Dict[str, Any], partial: bool = False) -> None:
        """Validate contact-specific rules"""
        if not partial and 'contact_rules' not in config:
            raise ConfigValidationError("Missing contact rules configuration")
            
        contact_rules = config.get('contact_rules', {})
        if contact_rules:
            for contact_id, rules in contact_rules.items():
                if not isinstance(rules, dict):
                    raise ConfigValidationError(f"Rules for contact {contact_id} must be a dictionary")
                    
                # Validate AEP date override if present
                if 'aep_date_override' in rules:
                    RuleConfigValidator.validate_date_override(
                        rules['aep_date_override'],
                        f"Contact {contact_id} AEP override"
                    )
                    
                # Validate post window rules if present
                if 'post_window_rules' in rules:
                    if not isinstance(rules['post_window_rules'], list):
                        raise ConfigValidationError(f"Post window rules for contact {contact_id} must be a list")
                        
                    for i, rule in enumerate(rules['post_window_rules']):
                        if 'condition' not in rule or 'override_date' not in rule:
                            raise ConfigValidationError(
                                f"Post window rule {i} for contact {contact_id} missing condition or override_date"
                            )
                            
                        condition = rule['condition']
                        if 'birth_month' in condition and not 1 <= condition['birth_month'] <= 12:
                            raise ConfigValidationError(
                                f"Invalid birth month in post window rule {i} for contact {contact_id}"
                            )
                            
                        RuleConfigValidator.validate_date_override(
                            rule['override_date'],
                            f"Contact {contact_id} post window rule {i}"
                        )

    @staticmethod
    def validate_global_rules(config: Dict[str, Any], partial: bool = False) -> None:
        """Validate global rules configuration"""
        if not partial and 'global_rules' not in config:
            raise ConfigValidationError("Missing global rules configuration")
            
        global_rules = config.get('global_rules', {})
        if global_rules:
            # Validate october_birthday_aep if present
            if 'october_birthday_aep' in global_rules:
                RuleConfigValidator.validate_date_override(
                    global_rules['october_birthday_aep'],
                    "October birthday AEP rule"
                )
                
            # Validate state specific rules
            state_specific = global_rules.get('state_specific_rules', {})
            for state, rules in state_specific.items():
                if 'post_window_period_days' in rules:
                    days = rules['post_window_period_days']
                    if not isinstance(days, int) or days < 0:
                        raise ConfigValidationError(
                            f"Post window period for state {state} must be a non-negative integer"
                        )
                        
                if 'leap_year_override' in rules:
                    RuleConfigValidator.validate_date_override(
                        rules['leap_year_override'],
                        f"State {state} leap year override"
                    )

    @classmethod
    def validate_config(cls, config_file: str, partial: bool = False) -> None:
        """Validate the entire configuration file"""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            raise ConfigValidationError(f"Error loading configuration file: {str(e)}")
            
        # Run all validation checks
        cls.validate_timing_constants(config, partial)
        cls.validate_aep_config(config, partial)
        cls.validate_state_rules(config, partial)
        cls.validate_contact_rules(config, partial)
        cls.validate_global_rules(config, partial) 