"""
Tests for Snowflake Data Warehouse Integration

This module contains comprehensive tests for the Snowflake integration components.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
import json

from .snowflake_client import SnowflakeClient
from .schema_manager import SchemaManager
from .s3_staging import S3StagingManager
from .snowpipe_manager import SnowpipeManager
from .integration import SnowflakeIntegration


class TestSnowflakeClient:
    """Test cases for SnowflakeClient"""
    
    @pytest.fixture
    def mock_config(self):
        return {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "role": "TEST_ROLE"
        }
    
    @pytest.fixture
    def snowflake_client(self, mock_config):
        with patch('src.streaming_pipeline.warehouse.snowflake_client.get_settings') as mock_settings:
            mock_settings.return_value = Mock(
                snowflake_account="test_account",
                snowflake_user="test_user",
                snowflake_password="test_password",
                snowflake_warehouse="TEST_WH",
                snowflake_database="TEST_DB",
                snowflake_schema="TEST_SCHEMA",
                snowflake_role="TEST_ROLE"
            )
            return SnowflakeClient(mock_config)
    
    @patch('snowflake.connector.connect')
    def test_connect_success(self, mock_connect, snowflake_client):
        """Test successful connection to Snowflake"""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connection = snowflake_client.connect()
        
        assert connection == mock_connection
        mock_connect.assert_called_once()
    
    @patch('snowflake.connector.connect')
    def test_connect_failure(self, mock_connect, snowflake_client):
        """Test connection failure handling"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            snowflake_client.connect()
    
    def test_execute_query_success(self, snowflake_client):
        """Test successful query execution"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.object(snowflake_client, 'connect', return_value=mock_connection):
            result = snowflake_client.execute_query("SELECT 1", fetch=False)
            
            assert result is None
            mock_cursor.execute.assert_called_once_with("SELECT 1")
            mock_connection.commit.assert_called_once()
    
    def test_execute_query_with_fetch(self, snowflake_client):
        """Test query execution with result fetching"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"col1": "value1"}]
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.object(snowflake_client, 'connect', return_value=mock_connection):
            result = snowflake_client.execute_query("SELECT * FROM test", fetch=True)
            
            assert result == [{"col1": "value1"}]
            mock_cursor.fetchall.assert_called_once()
    
    def test_check_table_exists(self, snowflake_client):
        """Test table existence check"""
        with patch.object(snowflake_client, 'execute_query') as mock_execute:
            mock_execute.return_value = [{"COUNT": 1}]
            
            exists = snowflake_client.check_table_exists("test_table")
            
            assert exists is True
            mock_execute.assert_called_once()


class TestS3StagingManager:
    """Test cases for S3StagingManager"""
    
    @pytest.fixture
    def mock_config(self):
        return {
            "bucket_name": "test-bucket",
            "region": "us-east-1",
            "staging_prefix": "staging/streaming/",
            "processed_prefix": "processed/streaming/",
            "error_prefix": "errors/streaming/"
        }
    
    @pytest.fixture
    def s3_staging(self, mock_config):
        with patch('src.streaming_pipeline.warehouse.s3_staging.get_settings') as mock_settings:
            mock_settings.return_value = Mock(
                s3_bucket_name="test-bucket",
                aws_region="us-east-1",
                aws_access_key_id="test_key",
                aws_secret_access_key="test_secret"
            )
            with patch('boto3.client'):
                return S3StagingManager(mock_config)
    
    def test_upload_dataframe_as_parquet(self, s3_staging):
        """Test DataFrame upload as Parquet"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'price': [150.0, 2500.0],
            'timestamp': [datetime.now(timezone.utc), datetime.now(timezone.utc)]
        })
        
        with patch.object(s3_staging.s3_client, 'put_object') as mock_put:
            s3_key = s3_staging.upload_dataframe_as_parquet(df, "test_table")
            
            assert s3_key.startswith("staging/streaming/test_table/")
            assert s3_key.endswith(".parquet")
            mock_put.assert_called_once()
    
    def test_upload_empty_dataframe(self, s3_staging):
        """Test handling of empty DataFrame"""
        df = pd.DataFrame()
        
        s3_key = s3_staging.upload_dataframe_as_parquet(df, "test_table")
        
        assert s3_key == ""
    
    def test_list_staged_files(self, s3_staging):
        """Test listing staged files"""
        mock_response = {
            'Contents': [
                {
                    'Key': 'staging/streaming/test_table/file1.parquet',
                    'Size': 1024,
                    'LastModified': datetime.now(timezone.utc),
                    'ETag': '"abc123"'
                }
            ]
        }
        
        with patch.object(s3_staging.s3_client, 'list_objects_v2', return_value=mock_response):
            with patch.object(s3_staging.s3_client, 'head_object', return_value={'Metadata': {}}):
                files = s3_staging.list_staged_files("test_table")
                
                assert len(files) == 1
                assert files[0]['key'] == 'staging/streaming/test_table/file1.parquet'
    
    def test_move_to_processed(self, s3_staging):
        """Test moving file to processed directory"""
        s3_key = "staging/streaming/test_table/file1.parquet"
        
        with patch.object(s3_staging.s3_client, 'copy_object') as mock_copy:
            with patch.object(s3_staging.s3_client, 'delete_object') as mock_delete:
                result = s3_staging.move_to_processed(s3_key)
                
                assert result is True
                mock_copy.assert_called_once()
                mock_delete.assert_called_once()


class TestSchemaManager:
    """Test cases for SchemaManager"""
    
    @pytest.fixture
    def schema_manager(self):
        mock_client = Mock(spec=SnowflakeClient)
        return SchemaManager(mock_client)
    
    def test_create_database_and_schemas(self, schema_manager):
        """Test database and schema creation"""
        schema_manager.create_database_and_schemas()
        
        # Verify that execute_query was called for each DDL statement
        assert schema_manager.client.execute_query.call_count >= 4
    
    def test_create_dimension_tables(self, schema_manager):
        """Test dimension table creation"""
        schema_manager.create_dimension_tables()
        
        # Verify that execute_query was called for each dimension table
        assert schema_manager.client.execute_query.call_count == 3
    
    def test_create_fact_tables(self, schema_manager):
        """Test fact table creation"""
        schema_manager.create_fact_tables()
        
        # Verify that execute_query was called for each fact table
        assert schema_manager.client.execute_query.call_count == 2
    
    def test_populate_date_dimension(self, schema_manager):
        """Test date dimension population"""
        schema_manager.populate_date_dimension("2023-01-01", "2023-12-31")
        
        schema_manager.client.execute_query.assert_called_once()
        call_args = schema_manager.client.execute_query.call_args[0][0]
        assert "INSERT INTO STREAMING.DIM_DATE" in call_args


class TestSnowpipeManager:
    """Test cases for SnowpipeManager"""
    
    @pytest.fixture
    def snowpipe_manager(self):
        mock_client = Mock(spec=SnowflakeClient)
        return SnowpipeManager(mock_client)
    
    def test_create_pipe(self, snowpipe_manager):
        """Test pipe creation"""
        snowpipe_manager.client.execute_query.return_value = None
        
        result = snowpipe_manager.create_pipe(
            pipe_name="TEST_PIPE",
            table_name="TEST_TABLE",
            stage_name="TEST_STAGE"
        )
        
        assert result is True
        snowpipe_manager.client.execute_query.assert_called_once()
        
        # Check that pipe was cached
        assert "TEST_PIPE" in snowpipe_manager.pipes
    
    def test_get_pipe_status(self, snowpipe_manager):
        """Test getting pipe status"""
        mock_status = {
            "PIPE_NAME": "TEST_PIPE",
            "IS_AUTOINGEST_ENABLED": True,
            "PIPE_EXECUTION_PAUSED": False
        }
        snowpipe_manager.client.execute_query.return_value = [mock_status]
        
        status = snowpipe_manager.get_pipe_status("TEST_PIPE")
        
        assert status == mock_status
    
    def test_pause_pipe(self, snowpipe_manager):
        """Test pausing a pipe"""
        result = snowpipe_manager.pause_pipe("TEST_PIPE")
        
        assert result is True
        snowpipe_manager.client.execute_query.assert_called_once()
        call_args = snowpipe_manager.client.execute_query.call_args[0][0]
        assert "PIPE_EXECUTION_PAUSED = TRUE" in call_args
    
    def test_monitor_pipe_health(self, snowpipe_manager):
        """Test pipe health monitoring"""
        # Mock statistics
        mock_stats = {
            'TOTAL_FILES': 100,
            'TOTAL_ROWS_LOADED': 10000,
            'ERROR_FILES': 2,
            'AVG_LOAD_TIME_SECONDS': 30,
            'LAST_LOAD_TIME': datetime.now(timezone.utc)
        }
        
        # Mock history
        mock_history = [
            {
                'FILE_NAME': 'test_file.parquet',
                'ERROR_SEEN': True,
                'ERROR_CODE': 'LOAD_ERROR',
                'ERROR_MESSAGE': 'Test error',
                'LAST_LOAD_TIME': datetime.now(timezone.utc)
            }
        ]
        
        with patch.object(snowpipe_manager, 'get_pipe_load_statistics', return_value=mock_stats):
            with patch.object(snowpipe_manager, 'get_pipe_execution_history', return_value=mock_history):
                health = snowpipe_manager.monitor_pipe_health("TEST_PIPE")
                
                assert health['pipe_name'] == "TEST_PIPE"
                assert health['total_files_processed'] == 100
                assert health['error_rate'] == 2.0
                assert health['health_status'] == 'HEALTHY'
                assert len(health['recent_errors']) == 1


class TestSnowflakeIntegration:
    """Test cases for SnowflakeIntegration"""
    
    @pytest.fixture
    def integration(self):
        with patch('src.streaming_pipeline.warehouse.integration.get_settings') as mock_settings:
            mock_settings.return_value = Mock(
                s3_bucket_name="test-bucket",
                aws_role_arn="arn:aws:iam::123456789012:role/test-role"
            )
            with patch.multiple(
                'src.streaming_pipeline.warehouse.integration',
                SnowflakeClient=Mock,
                SchemaManager=Mock,
                S3StagingManager=Mock,
                SnowpipeManager=Mock
            ):
                return SnowflakeIntegration()
    
    def test_initialize_warehouse(self, integration):
        """Test warehouse initialization"""
        integration.snowpipe_manager.setup_all_pipes.return_value = {
            'STOCK_PRICES_PIPE': True,
            'TRADING_VOLUME_PIPE': True,
            'DATA_QUALITY_PIPE': True
        }
        
        result = integration.initialize_warehouse()
        
        assert result is True
        assert integration.is_initialized is True
        integration.schema_manager.setup_complete_schema.assert_called_once()
        integration.snowpipe_manager.setup_all_pipes.assert_called_once()
    
    def test_load_stock_prices_data(self, integration):
        """Test loading stock prices data"""
        df = pd.DataFrame({
            'symbol': ['AAPL'],
            'price': [150.0],
            'timestamp': [datetime.now(timezone.utc)]
        })
        
        integration.s3_staging.upload_dataframe_as_parquet.return_value = "test/key.parquet"
        
        result = integration.load_stock_prices_data(df)
        
        assert result['success'] is True
        assert result['s3_key'] == "test/key.parquet"
        assert result['records_processed'] == 1
        integration.s3_staging.upload_dataframe_as_parquet.assert_called_once()
        integration.snowpipe_manager.refresh_pipe.assert_called_once_with("STOCK_PRICES_PIPE")
    
    def test_get_pipeline_health(self, integration):
        """Test getting pipeline health"""
        mock_pipe_health = {
            'pipe_name': 'TEST_PIPE',
            'health_status': 'HEALTHY',
            'total_files_processed': 100,
            'error_rate': 1.0
        }
        
        mock_staging_stats = {
            'total_files': 50,
            'total_size_bytes': 1024000
        }
        
        integration.snowpipe_manager.monitor_pipe_health.return_value = mock_pipe_health
        integration.s3_staging.get_staging_statistics.return_value = mock_staging_stats
        integration.snowflake_client.execute_query.return_value = []
        
        health = integration.get_pipeline_health()
        
        assert health['overall_status'] == 'HEALTHY'
        assert 'pipes' in health
        assert 's3_staging' in health
        assert 'recent_loads' in health
    
    def test_optimize_tables(self, integration):
        """Test table optimization"""
        result = integration.optimize_tables()
        
        # Should attempt to optimize 5 tables
        assert len(result) == 5
        assert integration.snowflake_client.optimize_table.call_count == 5


# Integration test fixtures and helpers
@pytest.fixture
def sample_stock_data():
    """Sample stock data for testing"""
    return pd.DataFrame({
        'symbol': ['AAPL', 'GOOGL', 'MSFT'],
        'open_price': [150.0, 2500.0, 300.0],
        'high_price': [155.0, 2550.0, 305.0],
        'low_price': [148.0, 2480.0, 298.0],
        'close_price': [152.0, 2520.0, 302.0],
        'volume': [1000000, 500000, 800000],
        'timestamp': [datetime.now(timezone.utc)] * 3
    })


@pytest.fixture
def sample_volume_data():
    """Sample volume data for testing"""
    return pd.DataFrame({
        'symbol': ['AAPL', 'GOOGL'],
        'volume': [1000000, 500000],
        'volume_weighted_price': [151.5, 2510.0],
        'trade_count': [5000, 2500],
        'timestamp': [datetime.now(timezone.utc)] * 2
    })


class TestIntegrationEndToEnd:
    """End-to-end integration tests"""
    
    @pytest.mark.integration
    def test_complete_data_pipeline(self, sample_stock_data):
        """Test complete data pipeline from upload to Snowflake"""
        # This would be a real integration test that requires actual AWS/Snowflake credentials
        # For now, we'll skip it in regular test runs
        pytest.skip("Requires real AWS/Snowflake credentials")
    
    @pytest.mark.integration
    def test_error_handling_pipeline(self):
        """Test error handling in the complete pipeline"""
        # Test various error scenarios
        pytest.skip("Requires real AWS/Snowflake credentials")


if __name__ == "__main__":
    pytest.main([__file__])