import pytest
from unittest.mock import Mock, patch
from gxt.adapters.bigquery import BigQueryAdapter


@pytest.mark.unit
class TestBigQueryAdapter:
    """Test the BigQuery adapter methods."""
    
    def test_hash_bucket_sql_generation(self):
        """Test hash bucket SQL generation."""
        adapter = BigQueryAdapter(client=Mock())
        
        result = adapter.hash_bucket_sql("user_id", "10")
        
        expected = "MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(user_id AS STRING),'::','10'))), 1000000)/1000000.0"
        assert result == expected
    
    def test_hash_bucket_sql_without_salt(self):
        """Test hash bucket SQL without salt."""
        adapter = BigQueryAdapter(client=Mock())
        
        result = adapter.hash_bucket_sql("user_id")
        
        expected = "MOD(ABS(FARM_FINGERPRINT(user_id)), 1000000)/1000000.0"
        assert result == expected
    
    def test_hash_bucket_sql_different_columns_and_salts(self):
        """Test hash bucket SQL with different parameters."""
        adapter = BigQueryAdapter(client=Mock())
        
        test_cases = [
            ("customer_id", "5", "MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(customer_id AS STRING),'::','5'))), 1000000)/1000000.0"),
            ("account_id", "exp1", "MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(account_id AS STRING),'::','exp1'))), 1000000)/1000000.0"),
            ("email", "", "MOD(ABS(FARM_FINGERPRINT(email)), 1000000)/1000000.0"),
        ]
        
        for column, salt, expected in test_cases:
            result = adapter.hash_bucket_sql(column, salt)
            assert result == expected
    
    def test_qualify_table_with_project_and_dataset(self):
        """Test table qualification with project and dataset."""
        adapter = BigQueryAdapter(project="my_project", client=Mock())
        
        result = adapter.qualify_table("my_dataset", "my_table")
        
        expected = "`my_project.my_dataset.my_table`"
        assert result == expected
    
    def test_qualify_table_without_project(self):
        """Test table qualification without project."""
        adapter = BigQueryAdapter(client=Mock())
        
        result = adapter.qualify_table("my_dataset", "my_table")
        
        expected = "`my_dataset.my_table`"
        assert result == expected
    
    def test_upsert_from_select_sql_generation(self):
        """Test MERGE SQL generation for upsert operations."""
        mock_client = Mock()
        adapter = BigQueryAdapter(client=mock_client)
        
        src_select_sql = "SELECT user_id, experiment_name, assignment_bucket FROM assignments"
        target_table = "my_project.my_dataset.assignments"
        key_columns = ["user_id", "experiment_name"]
        insert_columns = ["user_id", "experiment_name", "assignment_bucket"]
        
        # Mock the query execution and result
        mock_job = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))  # Empty iterator for rows
        mock_job.result.return_value = mock_result
        mock_client.query.return_value = mock_job
        
        # Call the method
        adapter.upsert_from_select(
            target_table=target_table,
            src_select_sql=src_select_sql,
            key_columns=key_columns,
            insert_columns=insert_columns
        )
        
        # Verify query was called
        mock_client.query.assert_called_once()
        
        # Get the actual SQL that was executed
        executed_sql = mock_client.query.call_args[0][0]
        
        # Verify MERGE statement structure
        assert "MERGE" in executed_sql
        assert target_table in executed_sql
        assert "USING" in executed_sql
        assert "ON" in executed_sql
        assert "WHEN NOT MATCHED THEN" in executed_sql
        assert "INSERT" in executed_sql
        
        # Verify columns are included
        for column in insert_columns:
            assert column in executed_sql
    
    @patch('gxt.adapters.bigquery.bigquery.Client')
    def test_from_profile_with_explicit_credentials(self, mock_client_class):
        """Test creating adapter from profile with explicit credentials."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        profile = {
            'project': 'test-project',
            'dataset': 'test_dataset',
            'credentials': '/path/to/keyfile.json'
        }
        
        adapter = BigQueryAdapter.from_profile(profile)
        
        # Verify client creation (note: current implementation uses ADC)
        mock_client_class.assert_called_once_with(project='test-project')
        
        # Verify adapter properties
        assert adapter.project == 'test-project'
        assert adapter.dataset == 'test_dataset'
        assert adapter.client == mock_client
    
    @patch('gxt.adapters.bigquery.bigquery.Client')
    def test_from_profile_with_adc(self, mock_client_class):
        """Test creating adapter from profile with Application Default Credentials."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        profile = {
            'project': 'test-project',
            'dataset': 'test_dataset'
            # No credentials - should use ADC
        }
        
        adapter = BigQueryAdapter.from_profile(profile)
        
        # Verify ADC client creation
        mock_client_class.assert_called_once_with(project='test-project')
        
        # Verify adapter properties
        assert adapter.project == 'test-project'
        assert adapter.dataset == 'test_dataset'
        assert adapter.client == mock_client