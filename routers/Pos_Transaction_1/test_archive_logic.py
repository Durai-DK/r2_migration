import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add parent dir to path to find modules
sys.path.append("/Users/DK/Desktop/Project/r2_migration")

# Mock core.creds before importing logic
sys.modules["core.creds"] = MagicMock()
sys.modules["logs.log_settings"] = MagicMock()
sys.modules["key_mapping"] = MagicMock()

# Import the target module
# We need to ensure we can patch read_json_data and store_json_data
import routers.Pos_Transaction.pos_creation as pos

class TestArchiveLogic(unittest.TestCase):
    
    def setUp(self):
        self.bucket = "test-bucket"
        self.mobile = "9999999999"
        self.imei = "123456789012345"
        self.key = f"mobile/{self.mobile}/imei/{self.imei}.json"
        
        self.new_record = {
            "bill_transaction_no__c": "INV-001",
            "customer_mobile__c": self.mobile,
            "item_remarks1__c": self.imei
        }

    @patch("routers.Pos_Transaction.pos_creation.read_json_data")
    @patch("routers.Pos_Transaction.pos_creation.store_json_data")
    def test_archive_from_summary(self, mock_store, mock_read):
        """Test Case 1: Record found in summary. Archive to transferd_data/{pri_id}.json"""
        
        pri_id = "INV-001"
        self.new_record["bill_transaction_no__c"] = pri_id
        
        def side_effect(bucket, key):
            if "mobile_summary.json" in key:
                return {
                    "records": [
                        {
                            "bill_transaction_no__c": pri_id,
                            "customer_mobile__c": self.mobile,
                            "data": "summary_data"
                        }
                    ]
                }
            return None

        mock_read.side_effect = side_effect
        
        pos.verify_and_archive_record(self.bucket, self.key, self.new_record, self.mobile, self.imei)
        
        # Should have archived the record from summary using PRI_ID as filename
        expected_archive_key = f"mobile/{self.mobile}/transferd_data/{pri_id}.json"
        mock_store.assert_called_once()
        args, _ = mock_store.call_args
        self.assertEqual(args[2], expected_archive_key)
        self.assertEqual(args[1]["data"], "summary_data")

    @patch("routers.Pos_Transaction.pos_creation.read_json_data")
    @patch("routers.Pos_Transaction.pos_creation.store_json_data")
    def test_archive_from_id_path(self, mock_store, mock_read):
        """Test Case 2: Not in summary, but found at id/{pri_id}.json"""
        
        pri_id = "INV-002"
        self.new_record["bill_transaction_no__c"] = pri_id
        
        def side_effect(bucket, key):
            if "mobile_summary.json" in key:
                return {"records": []} # Empty summary
            
            if f"id/{pri_id}.json" in key:
                 return {
                    "bill_transaction_no__c": pri_id,
                    "data": "id_data"
                }
            return None

        mock_read.side_effect = side_effect
        
        pos.verify_and_archive_record(self.bucket, self.key, self.new_record, self.mobile, self.imei)
        
        expected_archive_key = f"mobile/{self.mobile}/transferd_data/{pri_id}.json"
        mock_store.assert_called_once()
        args, _ = mock_store.call_args
        self.assertEqual(args[2], expected_archive_key)
        self.assertEqual(args[1]["data"], "id_data")

    @patch("routers.Pos_Transaction.pos_creation.read_json_data")
    @patch("routers.Pos_Transaction.pos_creation.store_json_data")
    def test_no_archive(self, mock_store, mock_read):
        """Test Case 3: Brand new record (No summary, No ID file)"""
        
        pri_id = "INV-003"
        self.new_record["bill_transaction_no__c"] = pri_id
        mock_read.return_value = None 
        
        pos.verify_and_archive_record(self.bucket, self.key, self.new_record, self.mobile, self.imei)
        
        mock_store.assert_not_called()

    @patch("routers.Pos_Transaction.pos_creation.r2_client")
    @patch("routers.Pos_Transaction.pos_creation.read_json_data")
    @patch("routers.Pos_Transaction.pos_creation.store_json_data")
    def test_rebuild_mobile_summary(self, mock_store, mock_read, mock_r2_client):
        """Test Case 4: Verify Rebuild Logic Aggregates Files"""
        
        # Mock R2 Client Paginator
        mock_s3 = MagicMock()
        mock_r2_client.return_value = mock_s3
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        
        # Mock List Objects response
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": f"mobile/{self.mobile}/id/INV-001.json"},
                    {"Key": f"mobile/{self.mobile}/id/INV-002.json"}
                ]
            }
        ]
        
        # Mock Read Data (using side_effect to return different content for files)
        def read_side_effect(bucket, key):
            if "INV-001" in key:
                return {"bill_transaction_no__c": "INV-001", "Bill_Grant_Total__c": "100"}
            if "INV-002" in key:
                return {"bill_transaction_no__c": "INV-002", "Bill_Grant_Total__c": "200"}
            return None
            
        mock_read.side_effect = read_side_effect
        
        pos.rebuild_mobile_summary(self.bucket, self.mobile)
        
        # Verify Store Summary called with correct totals
        mock_store.assert_called_once()
        args, _ = mock_store.call_args
        
        summary_key = f"mobile/{self.mobile}/mobile_summary.json"
        self.assertEqual(args[2], summary_key)
        
        summary_body = args[1]
        self.assertEqual(summary_body["total_records"], 2)
        self.assertEqual(summary_body["total_spends"], 300.0) # 100 + 200
        self.assertEqual(len(summary_body["records"]), 2)

if __name__ == "__main__":
    unittest.main()
