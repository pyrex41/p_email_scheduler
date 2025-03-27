import unittest
from datetime import date
import tempfile
import os
import yaml
from contact_rule_engine import ContactRuleEngine
from rule_config_validator import RuleConfigValidator, ConfigValidationError

class TestRuleConfigValidator(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        
    def create_test_config(self, config_data):
        """Helper to create a temporary config file"""
        config_path = os.path.join(self.test_dir, 'test_config.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
        return config_path

    def test_valid_config(self):
        """Test that a valid configuration passes validation"""
        config = {
            'timing_constants': {
                'birthday_email_days_before': 14,
                'effective_date_days_before': 30,
                'pre_window_exclusion_days': 60
            },
            'aep_config': {
                'default_dates': [
                    {'month': 8, 'day': 18},
                    {'month': 8, 'day': 25}
                ],
                'years': [2023, 2024]
            },
            'state_rules': {
                'CA': {
                    'type': 'birthday',
                    'window_before': 30,
                    'window_after': 30
                }
            },
            'contact_rules': {
                '502': {
                    'force_aep': True,
                    'aep_date_override': {
                        'month': 8,
                        'day': 25
                    }
                }
            },
            'global_rules': {
                'october_birthday_aep': {
                    'month': 8,
                    'day': 25
                }
            }
        }
        
        config_path = self.create_test_config(config)
        try:
            RuleConfigValidator.validate_config(config_path)
        except ConfigValidationError as e:
            self.fail(f"Validation failed for valid config: {str(e)}")

    def test_invalid_timing_constants(self):
        """Test validation of timing constants"""
        config = {
            'timing_constants': {
                'birthday_email_days_before': -1  # Invalid negative value
            }
        }
        
        config_path = self.create_test_config(config)
        with self.assertRaises(ConfigValidationError) as context:
            RuleConfigValidator.validate_config(config_path, partial=True)
        self.assertIn("must be a non-negative integer", str(context.exception))

    def test_invalid_date_override(self):
        """Test validation of date overrides"""
        config = {
            'contact_rules': {
                '502': {
                    'aep_date_override': {
                        'month': 13,  # Invalid month
                        'day': 25
                    }
                }
            }
        }
        
        config_path = self.create_test_config(config)
        with self.assertRaises(ConfigValidationError) as context:
            RuleConfigValidator.validate_config(config_path, partial=True)
        self.assertIn("Month must be between 1 and 12", str(context.exception))

    def test_invalid_state_rules(self):
        """Test validation of state rules"""
        config = {
            'state_rules': {
                'CA': {
                    'type': 'invalid_type'  # Invalid rule type
                }
            }
        }
        
        config_path = self.create_test_config(config)
        with self.assertRaises(ConfigValidationError) as context:
            RuleConfigValidator.validate_config(config_path, partial=True)
        self.assertIn("Invalid rule type", str(context.exception))

class TestContactRuleEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = {
            'timing_constants': {
                'birthday_email_days_before': 14,
                'effective_date_days_before': 30,
                'pre_window_exclusion_days': 60
            },
            'aep_config': {
                'default_dates': [
                    {'month': 8, 'day': 18},
                    {'month': 8, 'day': 25},
                    {'month': 9, 'day': 1},
                    {'month': 9, 'day': 7}
                ],
                'years': [2023, 2024]
            },
            'state_rules': {
                'CA': {
                    'type': 'birthday',
                    'window_before': 30,
                    'window_after': 30
                },
                'NY': {
                    'type': 'year_round'
                }
            },
            'contact_rules': {
                '502': {
                    'force_aep': True,
                    'aep_date_override': {
                        'month': 8,
                        'day': 25
                    }
                },
                '101': {
                    'aep_date_override': {
                        'month': 8,
                        'day': 18
                    },
                    'post_window_rules': [
                        {
                            'condition': {
                                'birth_month': 12,
                                'states': ['CA', 'LA']
                            },
                            'override_date': {
                                'month': 1,
                                'day': 15
                            }
                        }
                    ]
                }
            },
            'global_rules': {
                'october_birthday_aep': {
                    'month': 8,
                    'day': 25
                }
            }
        }
        
        self.config_path = os.path.join(self.test_dir, 'test_config.yaml')
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f)
            
        self.engine = ContactRuleEngine(config_file=self.config_path)

    def test_get_contact_rules(self):
        """Test retrieving contact-specific rules"""
        rules = self.engine.get_contact_rules('502')
        self.assertTrue(rules['force_aep'])
        self.assertEqual(rules['aep_date_override']['month'], 8)
        self.assertEqual(rules['aep_date_override']['day'], 25)

    def test_get_state_rules(self):
        """Test retrieving state-specific rules"""
        rules = self.engine.get_state_rules('CA')
        self.assertEqual(rules['type'], 'birthday')
        self.assertEqual(rules['window_before'], 30)
        self.assertEqual(rules['window_after'], 30)

    def test_is_year_round_enrollment_state(self):
        """Test year-round enrollment state check"""
        self.assertTrue(self.engine.is_year_round_enrollment_state('NY'))
        self.assertFalse(self.engine.is_year_round_enrollment_state('CA'))

    def test_get_aep_dates(self):
        """Test retrieving AEP dates for a year"""
        dates = self.engine.get_aep_dates(2024)
        self.assertEqual(len(dates), 4)
        self.assertEqual(dates[0], date(2024, 8, 18))
        self.assertEqual(dates[1], date(2024, 8, 25))
        self.assertEqual(dates[2], date(2024, 9, 1))
        self.assertEqual(dates[3], date(2024, 9, 7))

    def test_get_aep_date_override(self):
        """Test AEP date override for specific contacts"""
        # Test contact with explicit override
        contact_502 = {'id': '502', 'birth_date': '1960-05-15'}
        override_date = self.engine.get_aep_date_override(contact_502, date(2024, 1, 1))
        self.assertEqual(override_date, date(2024, 8, 25))

        # Test October birthday rule
        contact_october = {'id': '999', 'birth_date': '1960-10-15'}
        override_date = self.engine.get_aep_date_override(contact_october, date(2024, 1, 1))
        self.assertEqual(override_date, date(2024, 8, 25))

    def test_get_post_window_dates(self):
        """Test post-window dates calculation"""
        # Test contact with December birthday in CA
        contact_101 = {
            'id': '101',
            'birth_date': '1960-12-15',
            'state': 'CA'
        }
        post_window_dates = self.engine.get_post_window_dates(contact_101, date(2024, 1, 1))
        self.assertEqual(len(post_window_dates), 1)
        self.assertEqual(post_window_dates[0], date(2024, 1, 15))

if __name__ == '__main__':
    unittest.main() 