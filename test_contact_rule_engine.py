import unittest
from datetime import date
from contact_rule_engine import ContactRuleEngine

class TestContactRuleEngine(unittest.TestCase):
    def setUp(self):
        self.engine = ContactRuleEngine()

    def test_force_aep_email(self):
        # Test contact 502 should have forced AEP
        contact_502 = {"id": "502"}
        self.assertTrue(self.engine.should_force_aep_email(contact_502))
        
        # Test regular contact should not have forced AEP
        contact_regular = {"id": "123"}
        self.assertFalse(self.engine.should_force_aep_email(contact_regular))

    def test_aep_date_override(self):
        # Test contact 502 should get August 25 override
        contact_502 = {
            "id": "502",
            "birth_date": "1960-05-15"
        }
        current_date = date(2024, 1, 1)
        override_date = self.engine.get_aep_date_override(contact_502, current_date)
        self.assertEqual(override_date, date(2024, 8, 25))

        # Test October birthday should get August 25
        contact_october = {
            "id": "123",
            "birth_date": "1960-10-15"
        }
        override_date = self.engine.get_aep_date_override(contact_october, current_date)
        self.assertEqual(override_date, date(2024, 8, 25))

    def test_post_window_dates(self):
        # Test contact 101 with December birthday in CA
        contact_101 = {
            "id": "101",
            "birth_date": "1960-12-15",
            "state": "CA"
        }
        current_date = date(2024, 1, 1)
        post_window_dates = self.engine.get_post_window_dates(contact_101, current_date)
        self.assertEqual(len(post_window_dates), 1)
        self.assertEqual(post_window_dates[0], date(2024, 1, 15))

        # Test contact 103 with December birthday in LA
        contact_103 = {
            "id": "103",
            "birth_date": "1960-12-15",
            "state": "LA"
        }
        post_window_dates = self.engine.get_post_window_dates(contact_103, current_date)
        self.assertEqual(len(post_window_dates), 1)
        self.assertEqual(post_window_dates[0], date(2024, 1, 31))

        # Test leap year February birthday in NV
        contact_nv = {
            "id": "123",
            "birth_date": "1960-02-29",
            "state": "NV"
        }
        post_window_dates = self.engine.get_post_window_dates(contact_nv, current_date)
        self.assertEqual(len(post_window_dates), 1)
        self.assertEqual(post_window_dates[0], date(2024, 3, 31))

if __name__ == '__main__':
    unittest.main() 