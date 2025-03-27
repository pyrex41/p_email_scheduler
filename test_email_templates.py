"""
Test script for email templates.
Generates sample emails for each email type and validates content generation.
"""

import unittest
from datetime import date
from email_template_engine import EmailTemplateEngine

class TestEmailTemplates(unittest.TestCase):
    def setUp(self):
        self.template_engine = EmailTemplateEngine()
        self.sample_contact = {
            'id': '123',
            'first_name': 'John',
            'last_name': 'Doe',
            'state': 'CA',
            'birth_date': '1950-05-15'
        }
        self.email_date = date(2024, 5, 1)

    def test_birthday_email(self):
        """Test birthday email generation"""
        result = self.template_engine.render_email('birthday', self.sample_contact, self.email_date)
        self.assertIsNotNone(result)
        self.assertIn('subject', result)
        self.assertIn('body', result)
        self.assertIn('John', result['body'])

    def test_aep_email(self):
        """Test AEP email generation"""
        result = self.template_engine.render_email('aep', self.sample_contact, self.email_date)
        self.assertIsNotNone(result)
        self.assertIn('subject', result)
        self.assertIn('body', result)
        self.assertIn('Annual Enrollment Period', result['body'])

    def test_html_email(self):
        """Test HTML email generation"""
        result = self.template_engine.render_email('birthday', self.sample_contact, self.email_date, html=True)
        self.assertIsNotNone(result)
        self.assertIn('<!DOCTYPE html>', result)
        self.assertIn('John', result)

if __name__ == '__main__':
    unittest.main()