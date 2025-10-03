# test/test_lambda_handler_enrichment.py
import unittest
from processing.lambda_function_v2 import enrich_lead

class TestLeadEnrichment(unittest.TestCase):
    def test_enrich_lead_merges_crm_and_lookup(self):
        crm_event = {
            "lead_id": "lead_123",
            "data": {
                "display_name": "John Doe",
                "date_created": "2025-10-03",
                "status_label": "New"
            }
        }

        lookup_data = {
            "lead_email": "john@example.com",
            "lead_owner": "Alice Smith",
            "funnel": "Top of Funnel"
        }

        enriched = enrich_lead(crm_event, lookup_data)

        expected = {
            "Name": "John Doe",
            "Lead ID": "lead_123",
            "Created Date": "2025-10-03",
            "Label": "New",
            "Email": "john@example.com",
            "Lead Owner": "Alice Smith",
            "Funnel": "Top of Funnel"
        }

        self.assertEqual(enriched, expected)

if __name__ == "__main__":
    unittest.main()
