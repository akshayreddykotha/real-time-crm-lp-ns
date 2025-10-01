# test_lambda_handler.py - lead parsing
import json
import unittest
from unittest.mock import patch, MagicMock

from processing.lambda_function_v1 import lambda_handler


class TestLambdaHandler(unittest.TestCase):
    @patch("lambda_function.s3")  # mock the s3 client in your module
    @pytest.mark.unit
    def test_lambda_handler_parses_event_and_reads_s3(self, mock_s3):
        # Arrange
        fake_file_content = {"event": {"lead_id": "lead_123"}}
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(fake_file_content).encode("utf-8"))
        }

        event = {
            "Records": [
                {
                    "body": json.dumps({
                        "Message": json.dumps({
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "test-bucket"},
                                        "object": {"key": "leads/lead_123.json"}
                                    }
                                }
                            ]
                        })
                    })
                }
            ]
        }

        # Act
        lambda_handler(event, None)

        # Assert
        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="leads/lead_123.json"
        )
        # no assertion on print, but you could patch "builtins.print" if you want to check logs


if __name__ == "__main__":
    unittest.main()
