"""
Tests for Snowflake integration and database operations.

CRITICAL: Test database operations with test database as specified in PRP.
Verifies connection handling, retry logic, and batch insertion functionality.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime
import pandas as pd

from ..snowflake_client import SnowflakeClient, MockSnowflakeClient
from ..models import SnowflakeConfig, ProductData, DatabaseInsertResult


class TestSnowflakeConfig:
    """Test Snowflake configuration model."""
    
    def test_snowflake_config_validation(self):
        """Test SnowflakeConfig validation."""
        # Valid configuration
        config = SnowflakeConfig(
            account="test-account.snowflakecomputing.com",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            user="test_user",
            password="test_password",
            warehouse="TEST_WH"
        )
        
        assert config.account == "test-account.snowflakecomputing.com"
        assert config.database == "TEST_DB"
        assert config.schema == "TEST_SCHEMA"
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.warehouse == "TEST_WH"
        
        # Verify default table mappings
        assert "Whole Flower" in config.tables
        assert "Pre-Rolls" in config.tables
        assert "Ground & Shake" in config.tables
    
    def test_snowflake_config_required_fields(self):
        """Test that required fields raise validation errors."""
        with pytest.raises(ValueError, match="Field cannot be empty"):
            SnowflakeConfig(
                account="",  # Empty account should fail
                database="TEST_DB",
                schema="TEST_SCHEMA",
                user="test_user",
                password="test_password"
            )
    
    def test_snowflake_config_default_values(self):
        """Test default values in SnowflakeConfig."""
        config = SnowflakeConfig(
            user="test_user",
            password="test_password"
        )
        
        # Should use default values
        assert config.account == "CURALEAF-CURAPROD.snowflakecomputing.com"
        assert config.database == "SANDBOX_EDW"
        assert config.schema == "ANALYTICS"
        assert config.warehouse == "COMPUTE_WH"


@patch('agents.dispensary_scraper.snowflake_client.SNOWFLAKE_AVAILABLE', True)
class TestSnowflakeClient:
    """Test SnowflakeClient with mocked Snowflake connector."""
    
    @pytest.fixture
    def snowflake_config(self):
        """Test Snowflake configuration."""
        return SnowflakeConfig(
            account="test-account.snowflakecomputing.com",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            user="test_user",
            password="test_password",
            warehouse="TEST_WH"
        )
    
    @pytest.fixture
    def mock_snowflake_connector(self):
        """Mock the snowflake.connector module."""
        with patch('agents.dispensary_scraper.snowflake_client.snowflake.connector') as mock_connector:
            mock_connection = Mock()
            mock_connection.is_closed.return_value = False
            mock_connection.cursor.return_value = Mock()
            mock_connection.commit = Mock()
            mock_connection.rollback = Mock()
            mock_connector.connect.return_value = mock_connection
            yield mock_connector, mock_connection
    
    def test_snowflake_client_initialization(self, snowflake_config):
        """Test SnowflakeClient initialization."""
        client = SnowflakeClient(snowflake_config)
        
        assert client.config == snowflake_config
        assert client._connection is None
        assert "account" in client._connection_params
        assert "user" in client._connection_params
        assert "password" in client._connection_params
    
    @pytest.mark.asyncio
    async def test_connection_with_retry_logic(self, snowflake_config, mock_snowflake_connector):
        """Test connection establishment with retry logic."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Test successful connection on first try
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        assert client._connection is not None
        mock_connector.connect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connection_retry_on_failure(self, snowflake_config, mock_snowflake_connector):
        """Test connection retry logic on failures."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Simulate failures then success
        from agents.dispensary_scraper.snowflake_client import DatabaseError
        mock_connector.connect.side_effect = [
            DatabaseError("Connection timeout"),
            DatabaseError("Network error"),
            mock_connection  # Success on third try
        ]
        
        client = SnowflakeClient(snowflake_config)
        
        # Should succeed after retries
        await client.connect()
        assert mock_connector.connect.call_count == 3
        assert client._connection == mock_connection
    
    @pytest.mark.asyncio
    async def test_connection_failure_after_retries(self, snowflake_config, mock_snowflake_connector):
        """Test connection failure after all retries exhausted."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Simulate all failures
        from agents.dispensary_scraper.snowflake_client import DatabaseError
        mock_connector.connect.side_effect = DatabaseError("Persistent connection error")
        
        client = SnowflakeClient(snowflake_config)
        
        # Should raise exception after all retries
        with pytest.raises(DatabaseError):
            await client.connect()
        
        assert mock_connector.connect.call_count == 3  # Tried 3 times
    
    @pytest.mark.asyncio
    async def test_insert_products_success(self, snowflake_config, mock_snowflake_connector, sample_products_list):
        """Test successful product insertion."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Mock write_pandas function
        with patch('agents.dispensary_scraper.snowflake_client.write_pandas') as mock_write_pandas:
            mock_write_pandas.return_value = (True, 1, len(sample_products_list), None)
            
            client = SnowflakeClient(snowflake_config)
            await client.connect()
            
            result = await client.insert_products(sample_products_list, "TEST_TABLE")
            
            assert result.success
            assert result.rows_inserted == len(sample_products_list)
            assert result.table_name == "TEST_TABLE"
            assert result.category == sample_products_list[0].subcategory
            
            # Verify write_pandas was called correctly
            mock_write_pandas.assert_called_once()
            call_args = mock_write_pandas.call_args
            assert call_args[1]['database'] == snowflake_config.database
            assert call_args[1]['schema'] == snowflake_config.schema
    
    @pytest.mark.asyncio
    async def test_insert_products_with_retry(self, snowflake_config, mock_snowflake_connector, sample_products_list):
        """Test product insertion with retry logic."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Mock write_pandas to fail then succeed
        with patch('agents.dispensary_scraper.snowflake_client.write_pandas') as mock_write_pandas:
            from agents.dispensary_scraper.snowflake_client import DatabaseError
            mock_write_pandas.side_effect = [
                DatabaseError("Temporary error"),
                (True, 1, len(sample_products_list), None)  # Success on retry
            ]
            
            client = SnowflakeClient(snowflake_config)
            await client.connect()
            
            result = await client.insert_products(sample_products_list, "TEST_TABLE")
            
            assert result.success
            assert result.rows_inserted == len(sample_products_list)
            assert mock_write_pandas.call_count == 2  # Original attempt + 1 retry
    
    @pytest.mark.asyncio
    async def test_insert_products_failure_after_retries(self, snowflake_config, mock_snowflake_connector, sample_products_list):
        """Test product insertion failure after all retries."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Mock write_pandas to always fail
        with patch('agents.dispensary_scraper.snowflake_client.write_pandas') as mock_write_pandas:
            from agents.dispensary_scraper.snowflake_client import DatabaseError
            mock_write_pandas.side_effect = DatabaseError("Persistent database error")
            
            client = SnowflakeClient(snowflake_config)
            await client.connect()
            
            result = await client.insert_products(sample_products_list, "TEST_TABLE")
            
            assert not result.success
            assert "Persistent database error" in result.error_message
            assert result.rows_inserted == 0
            assert mock_write_pandas.call_count == 3  # Original + 2 retries
    
    @pytest.mark.asyncio
    async def test_insert_empty_products_list(self, snowflake_config, mock_snowflake_connector):
        """Test insertion of empty products list."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.insert_products([], "TEST_TABLE")
        
        assert result.success
        assert result.rows_inserted == 0
        assert result.table_name == "TEST_TABLE"
        assert result.category == "unknown"
    
    @pytest.mark.asyncio
    async def test_insert_products_by_category(self, snowflake_config, mock_snowflake_connector, sample_products_list):
        """Test inserting products organized by category."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Organize products by category
        products_by_category = {}
        for product in sample_products_list:
            category = product.subcategory
            if category not in products_by_category:
                products_by_category[category] = []
            products_by_category[category].append(product)
        
        # Mock write_pandas
        with patch('agents.dispensary_scraper.snowflake_client.write_pandas') as mock_write_pandas:
            mock_write_pandas.return_value = (True, 1, 1, None)  # Success for each call
            
            client = SnowflakeClient(snowflake_config)
            await client.connect()
            
            results = await client.insert_products_by_category(products_by_category)
            
            # Should have results for each category
            assert len(results) == len(products_by_category)
            
            for category, result in results.items():
                assert result.success
                assert result.category == category
                # Verify table mapping was used
                expected_table = snowflake_config.tables.get(category, f"unknown_table_{category}")
                if category in snowflake_config.tables:
                    assert result.table_name == snowflake_config.tables[category]
    
    @pytest.mark.asyncio
    async def test_test_connection_success(self, snowflake_config, mock_snowflake_connector):
        """Test successful connection test."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Setup cursor mock
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [1]
        mock_connection.cursor.return_value = mock_cursor
        
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.test_connection()
        
        assert result is True
        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_cursor.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_test_connection_failure(self, snowflake_config, mock_snowflake_connector):
        """Test connection test failure."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Setup cursor to raise exception
        mock_connection.cursor.side_effect = Exception("Connection lost")
        
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.test_connection()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_table_info_success(self, snowflake_config, mock_snowflake_connector):
        """Test successful table information retrieval."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Setup cursor with table info
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ("STATE", "VARCHAR", "NO"),
            ("STORE", "VARCHAR", "NO"),
            ("PRICE", "FLOAT", "YES")
        ]
        mock_connection.cursor.return_value = mock_cursor
        
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.get_table_info("TEST_TABLE")
        
        assert result is not None
        assert result["table_name"] == "TEST_TABLE"
        assert result["database"] == snowflake_config.database
        assert result["schema"] == snowflake_config.schema
        assert len(result["columns"]) == 3
        
        # Verify column info structure
        for col in result["columns"]:
            assert "name" in col
            assert "type" in col
            assert "nullable" in col
    
    @pytest.mark.asyncio
    async def test_get_table_info_not_found(self, snowflake_config, mock_snowflake_connector):
        """Test table information retrieval for non-existent table."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        # Setup cursor with no results
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value = mock_cursor
        
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.get_table_info("NON_EXISTENT_TABLE")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_close_connection(self, snowflake_config, mock_snowflake_connector):
        """Test connection cleanup."""
        mock_connector, mock_connection = mock_snowflake_connector
        
        client = SnowflakeClient(snowflake_config)
        await client.connect()
        
        # Verify connection is set
        assert client._connection is not None
        
        await client.close()
        
        # Verify close was called and connection reset
        mock_connection.close.assert_called_once()
        assert client._connection is None


class TestMockSnowflakeClient:
    """Test MockSnowflakeClient for testing and development."""
    
    def test_mock_client_initialization(self, snowflake_config):
        """Test MockSnowflakeClient initialization."""
        client = MockSnowflakeClient(snowflake_config)
        
        assert client.config == snowflake_config
        assert not client.connected
    
    @pytest.mark.asyncio
    async def test_mock_connect(self, snowflake_config):
        """Test mock connection."""
        client = MockSnowflakeClient(snowflake_config)
        
        await client.connect()
        
        assert client.connected
    
    @pytest.mark.asyncio
    async def test_mock_insert_products(self, snowflake_config, sample_products_list):
        """Test mock product insertion."""
        client = MockSnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.insert_products(sample_products_list, "MOCK_TABLE")
        
        assert result.success
        assert result.rows_inserted == len(sample_products_list)
        assert result.table_name == "MOCK_TABLE"
        assert result.category == sample_products_list[0].subcategory
    
    @pytest.mark.asyncio
    async def test_mock_insert_by_category(self, snowflake_config, sample_products_list):
        """Test mock category-based insertion."""
        client = MockSnowflakeClient(snowflake_config)
        await client.connect()
        
        # Group products by category
        products_by_category = {"Whole Flower": sample_products_list[:3], "Pre-Rolls": sample_products_list[3:]}
        
        results = await client.insert_products_by_category(products_by_category)
        
        assert len(results) == 2
        assert "Whole Flower" in results
        assert "Pre-Rolls" in results
        
        for category, result in results.items():
            assert result.success
            assert result.category == category
    
    @pytest.mark.asyncio
    async def test_mock_test_connection(self, snowflake_config):
        """Test mock connection test."""
        client = MockSnowflakeClient(snowflake_config)
        
        # Should return False when not connected
        assert not await client.test_connection()
        
        await client.connect()
        
        # Should return True when connected
        assert await client.test_connection()
    
    @pytest.mark.asyncio
    async def test_mock_get_table_info(self, snowflake_config):
        """Test mock table information."""
        client = MockSnowflakeClient(snowflake_config)
        
        result = await client.get_table_info("ANY_TABLE")
        
        assert result is not None
        assert result["table_name"] == "ANY_TABLE"
        assert result["database"] == snowflake_config.database
        assert len(result["columns"]) > 0
        
        # Verify has expected columns
        column_names = [col["name"] for col in result["columns"]]
        assert "STATE" in column_names
        assert "STORE" in column_names
        assert "PRICE" in column_names
    
    @pytest.mark.asyncio
    async def test_mock_close(self, snowflake_config):
        """Test mock connection close."""
        client = MockSnowflakeClient(snowflake_config)
        await client.connect()
        
        assert client.connected
        
        await client.close()
        
        assert not client.connected


class TestDatabaseInsertResult:
    """Test DatabaseInsertResult model."""
    
    def test_database_insert_result_success(self):
        """Test successful database insert result."""
        result = DatabaseInsertResult(
            table_name="TEST_TABLE",
            category="Whole Flower",
            success=True,
            rows_inserted=100,
            duration_seconds=2.5
        )
        
        assert result.success
        assert result.rows_inserted == 100
        assert result.rows_failed == 0
        assert result.error_message is None
        assert result.duration_seconds == 2.5
        assert result.inserted_at is not None
    
    def test_database_insert_result_failure(self):
        """Test failed database insert result."""
        result = DatabaseInsertResult(
            table_name="TEST_TABLE",
            category="Pre-Rolls",
            success=False,
            rows_failed=50,
            error_message="Connection timeout",
            duration_seconds=10.0
        )
        
        assert not result.success
        assert result.rows_inserted == 0
        assert result.rows_failed == 50
        assert result.error_message == "Connection timeout"
        assert result.duration_seconds == 10.0


@pytest.mark.integration
class TestSnowflakeIntegration:
    """Integration tests for Snowflake operations."""
    
    @pytest.mark.asyncio
    async def test_complete_workflow_with_mock_client(self, snowflake_config, sample_products_list):
        """Test complete workflow with mock Snowflake client."""
        client = MockSnowflakeClient(snowflake_config)
        await client.connect()
        
        # Test connection
        assert await client.test_connection()
        
        # Get table info
        table_info = await client.get_table_info("TL_Scrape_WHOLE_FLOWER")
        assert table_info is not None
        
        # Insert products
        result = await client.insert_products(sample_products_list, "TL_Scrape_WHOLE_FLOWER")
        assert result.success
        assert result.rows_inserted > 0
        
        # Close connection
        await client.close()
        assert not client.connected
    
    @pytest.mark.asyncio
    async def test_data_types_and_pandas_integration(self, snowflake_config, sample_products_list):
        """Test data type handling and pandas integration."""
        # Verify that ProductData can be converted to DataFrame
        df = pd.DataFrame([product.model_dump() for product in sample_products_list])
        
        # Check DataFrame structure
        assert len(df) == len(sample_products_list)
        assert "state" in df.columns
        assert "store" in df.columns
        assert "subcategory" in df.columns
        assert "price" in df.columns
        assert "grams" in df.columns
        assert "scraped_at" in df.columns
        
        # Verify data types can be handled
        assert df["price"].dtype in ["float64", "object"]  # Prices should be numeric or convertible
        assert df["grams"].dtype in ["float64", "object"]  # Grams should be numeric or convertible
        
        # Test that scraped_at can be converted to datetime
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
        assert df["scraped_at"].dtype.name.startswith("datetime")
        
        # Mock client should handle this DataFrame
        client = MockSnowflakeClient(snowflake_config)
        await client.connect()
        
        result = await client.insert_products(sample_products_list, "TEST_TABLE")
        assert result.success