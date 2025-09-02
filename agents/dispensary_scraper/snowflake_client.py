"""
Snowflake integration client with connection pooling and retry logic.

CRITICAL: Follows the exact pattern from PRP with write_pandas for efficient
bulk insert operations and proper connection management with retry logic.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
import pandas as pd

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from snowflake.connector import ProgrammingError, DatabaseError, InterfaceError
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    # Create dummy classes for when Snowflake is not available
    class ProgrammingError(Exception):
        pass
    class DatabaseError(Exception):
        pass
    class InterfaceError(Exception):
        pass

from .models import SnowflakeConfig, ProductData, DatabaseInsertResult

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """
    Snowflake client with connection pooling, retry logic, and batch insertion.
    
    CRITICAL: Uses write_pandas for efficient bulk operations and implements
    exponential backoff retry pattern as specified in PRP pseudocode.
    """
    
    def __init__(self, config: SnowflakeConfig):
        """
        Initialize Snowflake client with configuration.
        
        Args:
            config: Snowflake connection configuration
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is not installed. "
                "Install with: pip install snowflake-connector-python[pandas]"
            )
        
        self.config = config
        self._connection = None
        self._connection_params = {
            "account": config.account,
            "user": config.user,
            "password": config.password,
            "database": config.database,
            "schema": config.schema,
            "warehouse": config.warehouse
        }
        
        if config.role:
            self._connection_params["role"] = config.role
        
        logger.info(f"Initialized Snowflake client for account: {config.account}")
    
    async def connect(self) -> None:
        """
        Establish connection to Snowflake with retry logic.
        
        CRITICAL: Connection can timeout, need retry logic as specified in PRP.
        """
        for attempt in range(3):
            try:
                self._connection = snowflake.connector.connect(**self._connection_params)
                logger.info("Successfully connected to Snowflake")
                return
            except (DatabaseError, InterfaceError) as e:
                if attempt == 2:  # Last attempt
                    logger.error(f"Failed to connect to Snowflake after 3 attempts: {e}")
                    raise
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}, retrying...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    def get_connection(self):
        """
        Get current connection, establishing if needed.
        
        Returns:
            snowflake.connector.Connection: Active connection
            
        Raises:
            Exception: If connection cannot be established
        """
        if not self._connection or self._connection.is_closed():
            raise Exception("Connection not established. Call connect() first.")
        return self._connection
    
    @contextmanager
    def connection_context(self):
        """
        Context manager for connection handling with automatic cleanup.
        
        Yields:
            snowflake.connector.Connection: Active connection
        """
        conn = self.get_connection()
        try:
            yield conn
        except Exception:
            # Rollback any pending transactions on error
            try:
                conn.rollback()
            except:
                pass
            raise
        finally:
            # Commit successful transactions
            try:
                conn.commit()
            except:
                pass
    
    async def insert_products(
        self, 
        products: List[ProductData], 
        table_name: str
    ) -> DatabaseInsertResult:
        """
        Insert products into Snowflake table with batch processing and retry logic.
        
        CRITICAL: Uses write_pandas for efficient bulk insert as specified in PRP.
        
        Args:
            products: List of product data to insert
            table_name: Target Snowflake table name
            
        Returns:
            DatabaseInsertResult: Result of insertion operation
        """
        start_time = asyncio.get_event_loop().time()
        
        if not products:
            return DatabaseInsertResult(
                table_name=table_name,
                category="unknown",
                success=True,
                rows_inserted=0,
                duration_seconds=0
            )
        
        # Determine category from first product
        category = products[0].subcategory if products else "unknown"
        
        # PATTERN: Connection management with retry logic (from PRP pseudocode)
        for attempt in range(3):
            try:
                with self.connection_context() as conn:
                    # CRITICAL: Batch insert for performance using pandas
                    df = pd.DataFrame([p.model_dump() for p in products])
                    
                    # Ensure scraped_at column is properly formatted
                    if 'scraped_at' in df.columns:
                        df['scraped_at'] = pd.to_datetime(df['scraped_at'])
                    
                    logger.info(f"Inserting {len(df)} rows into {table_name}")
                    
                    # Use write_pandas for efficient bulk insert
                    success, nchunks, nrows, _ = write_pandas(
                        conn, 
                        df, 
                        table_name,
                        database=self.config.database,
                        schema=self.config.schema,
                        auto_create_table=False,  # Tables should already exist
                        overwrite=False
                    )
                    
                    end_time = asyncio.get_event_loop().time()
                    duration = end_time - start_time
                    
                    if success:
                        logger.info(f"Successfully inserted {nrows} rows into {table_name}")
                        return DatabaseInsertResult(
                            table_name=table_name,
                            category=category,
                            success=True,
                            rows_inserted=nrows,
                            duration_seconds=duration
                        )
                    else:
                        raise Exception("write_pandas returned success=False")
                        
            except (ProgrammingError, DatabaseError, InterfaceError) as e:
                logger.error(f"Snowflake error on attempt {attempt + 1}: {e}")
                if attempt == 2:  # Last attempt
                    end_time = asyncio.get_event_loop().time()
                    return DatabaseInsertResult(
                        table_name=table_name,
                        category=category,
                        success=False,
                        error_message=str(e),
                        duration_seconds=end_time - start_time
                    )
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Unexpected error during insert: {e}")
                if attempt == 2:  # Last attempt
                    end_time = asyncio.get_event_loop().time()
                    return DatabaseInsertResult(
                        table_name=table_name,
                        category=category,
                        success=False,
                        error_message=str(e),
                        duration_seconds=end_time - start_time
                    )
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        # Should never reach here, but safety fallback
        return DatabaseInsertResult(
            table_name=table_name,
            category=category,
            success=False,
            error_message="Exhausted all retry attempts"
        )
    
    async def insert_products_by_category(
        self,
        products_by_category: Dict[str, List[ProductData]]
    ) -> Dict[str, DatabaseInsertResult]:
        """
        Insert products organized by category into their respective tables.
        
        Args:
            products_by_category: Dictionary mapping category to list of products
            
        Returns:
            Dictionary mapping category to insertion results
        """
        results = {}
        
        for category, products in products_by_category.items():
            if category in self.config.tables:
                table_name = self.config.tables[category]
                result = await self.insert_products(products, table_name)
                results[category] = result
                logger.info(f"Category {category}: {result.rows_inserted} rows inserted into {table_name}")
            else:
                logger.warning(f"No table mapping found for category: {category}")
                results[category] = DatabaseInsertResult(
                    table_name="unknown",
                    category=category,
                    success=False,
                    error_message=f"No table mapping for category: {category}"
                )
        
        return results
    
    async def test_connection(self) -> bool:
        """
        Test connection to Snowflake.
        
        Returns:
            bool: True if connection is working
        """
        try:
            with self.connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    async def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a Snowflake table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information or None if table doesn't exist
        """
        try:
            with self.connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_database = '{self.config.database}'
                    AND table_schema = '{self.config.schema}'
                    AND table_name = '{table_name.upper()}'
                    ORDER BY ordinal_position
                """)
                
                columns = cursor.fetchall()
                cursor.close()
                
                if not columns:
                    return None
                
                return {
                    "table_name": table_name,
                    "database": self.config.database,
                    "schema": self.config.schema,
                    "columns": [
                        {
                            "name": col[0],
                            "type": col[1],
                            "nullable": col[2] == "YES"
                        }
                        for col in columns
                    ]
                }
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            return None
    
    async def close(self) -> None:
        """
        Close Snowflake connection.
        
        CRITICAL: Proper cleanup prevents resource leaks.
        """
        if self._connection and not self._connection.is_closed():
            try:
                self._connection.close()
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing Snowflake connection: {e}")
        
        self._connection = None


class MockSnowflakeClient:
    """
    Mock Snowflake client for testing and development.
    
    Provides the same interface as SnowflakeClient but doesn't actually
    connect to Snowflake. Useful for testing and development.
    """
    
    def __init__(self, config: SnowflakeConfig):
        """Initialize mock client."""
        self.config = config
        self.connected = False
        logger.info("Initialized mock Snowflake client")
    
    async def connect(self) -> None:
        """Mock connection."""
        self.connected = True
        logger.info("Mock Snowflake connection established")
    
    async def insert_products(
        self, 
        products: List[ProductData], 
        table_name: str
    ) -> DatabaseInsertResult:
        """Mock product insertion."""
        category = products[0].subcategory if products else "unknown"
        
        logger.info(f"Mock insert: {len(products)} products into {table_name}")
        
        return DatabaseInsertResult(
            table_name=table_name,
            category=category,
            success=True,
            rows_inserted=len(products),
            duration_seconds=0.1
        )
    
    async def insert_products_by_category(
        self,
        products_by_category: Dict[str, List[ProductData]]
    ) -> Dict[str, DatabaseInsertResult]:
        """Mock category insertion."""
        results = {}
        
        for category, products in products_by_category.items():
            table_name = self.config.tables.get(category, f"mock_table_{category.lower().replace(' ', '_')}")
            results[category] = await self.insert_products(products, table_name)
        
        return results
    
    async def test_connection(self) -> bool:
        """Mock connection test."""
        return self.connected
    
    async def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Mock table info."""
        return {
            "table_name": table_name,
            "database": self.config.database,
            "schema": self.config.schema,
            "columns": [
                {"name": "STATE", "type": "VARCHAR", "nullable": False},
                {"name": "STORE", "type": "VARCHAR", "nullable": False},
                {"name": "SUBCATEGORY", "type": "VARCHAR", "nullable": False},
                {"name": "NAME", "type": "VARCHAR", "nullable": False},
                {"name": "BRAND", "type": "VARCHAR", "nullable": True},
                {"name": "STRAIN_TYPE", "type": "VARCHAR", "nullable": True},
                {"name": "THC_PCT", "type": "FLOAT", "nullable": True},
                {"name": "SIZE_RAW", "type": "VARCHAR", "nullable": True},
                {"name": "GRAMS", "type": "FLOAT", "nullable": True},
                {"name": "PRICE", "type": "FLOAT", "nullable": True},
                {"name": "PRICE_PER_G", "type": "FLOAT", "nullable": True},
                {"name": "URL", "type": "VARCHAR", "nullable": True},
                {"name": "SCRAPED_AT", "type": "TIMESTAMP_NTZ", "nullable": False},
            ]
        }
    
    async def close(self) -> None:
        """Mock close."""
        self.connected = False
        logger.info("Mock Snowflake connection closed")