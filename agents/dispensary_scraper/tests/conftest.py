"""
Test configuration and fixtures for dispensary scraper tests.

CRITICAL: Setup test fixtures and mocked dependencies as specified in PRP.
Uses TestModel for agent testing without API calls.
"""

import pytest
import asyncio
from typing import List, Dict, Any
from unittest.mock import Mock, AsyncMock
import os
from datetime import datetime

# Set test environment variables before importing modules
os.environ["LLM_API_KEY"] = "test_key"
os.environ["SNOWFLAKE_USER"] = "test_user" 
os.environ["SNOWFLAKE_PASSWORD"] = "test_password"

from ..models import (
    ProductData, SnowflakeConfig, AgentConfig, ScrapingResult
)
from ..dependencies import MockScrapingDependencies
from ..snowflake_client import MockSnowflakeClient


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def snowflake_config():
    """Test Snowflake configuration."""
    return SnowflakeConfig(
        account="test-account.snowflakecomputing.com",
        database="TEST_DB",
        schema="TEST_SCHEMA", 
        user="test_user",
        password="test_password",
        warehouse="TEST_WH",
        tables={
            "Whole Flower": "TEST_WHOLE_FLOWER",
            "Pre-Rolls": "TEST_PRE_ROLLS",
            "Ground & Shake": "TEST_GROUND_SHAKE"
        }
    )


@pytest.fixture
def agent_config():
    """Test agent configuration."""
    return AgentConfig(
        categories={
            "Whole Flower": "https://test.trulieve.com/category/flower/whole-flower",
            "Pre-Rolls": "https://test.trulieve.com/category/flower/pre-rolls",
            "Ground & Shake": "https://test.trulieve.com/category/flower/ground-shake"
        },
        min_delay_ms=100,  # Faster for tests
        max_delay_ms=200,
        page_timeout_ms=5000,
        max_retries=2,
        min_expected_products=10,  # Lower for tests
        min_expected_stores=5
    )


@pytest.fixture
def mock_snowflake_client():
    """Mock Snowflake client for testing."""
    return MockSnowflakeClient(snowflake_config=snowflake_config())


@pytest.fixture
def mock_scraping_dependencies():
    """Mock scraping dependencies for testing."""
    return MockScrapingDependencies(session_id="test-session-123")


@pytest.fixture
def sample_product_data():
    """Sample product data for testing."""
    return ProductData(
        state="FL",
        store="Test Store - Miami, FL",
        subcategory="Whole Flower",
        name="Blue Dream",
        brand="Test Brand",
        strain_type="Hybrid",
        thc_pct=22.5,
        size_raw="3.5g",
        grams=3.5,
        price=45.00,
        price_per_g=12.86,
        url="https://test.trulieve.com/product/blue-dream-35g",
        scraped_at=datetime.now()
    )


@pytest.fixture  
def sample_products_list(sample_product_data):
    """List of sample products for testing."""
    products = []
    
    # Create variations
    base_product = sample_product_data.model_copy()
    
    # Different sizes
    for size, grams in [("1g", 1.0), ("7g", 7.0), ("14g", 14.0)]:
        product = base_product.model_copy()
        product.size_raw = size
        product.grams = grams
        product.price = grams * 12.50  # $12.50 per gram
        product.price_per_g = 12.50
        products.append(product)
    
    # Different stores
    for i, store in enumerate(["Test Store - Tampa, FL", "Test Store - Orlando, FL"]):
        product = base_product.model_copy()
        product.store = store
        product.name = f"Green Crack {i+1}"
        product.price = 40.00 + (i * 5)  # Varying prices
        product.price_per_g = product.price / product.grams
        products.append(product)
    
    # Different categories
    for category in ["Pre-Rolls", "Ground & Shake"]:
        product = base_product.model_copy()
        product.subcategory = category
        product.name = f"Test {category} Product"
        products.append(product)
    
    return products


@pytest.fixture
def mock_playwright_page():
    """Mock Playwright page for testing."""
    page = AsyncMock()
    
    # Mock common methods
    page.goto = AsyncMock()
    page.locator.return_value = mock_playwright_locator()
    page.get_by_role.return_value = mock_playwright_locator()
    page.wait_for_timeout = AsyncMock()
    page.close = AsyncMock()
    page.inner_text = AsyncMock(return_value="Test page content $25.99")
    
    # Mock mouse
    mouse = Mock()
    mouse.wheel = AsyncMock()
    page.mouse = mouse
    
    return page


@pytest.fixture
def mock_playwright_locator():
    """Mock Playwright locator for testing."""
    locator = AsyncMock()
    
    # Mock methods
    locator.all = AsyncMock(return_value=[])
    locator.count = AsyncMock(return_value=0)
    locator.text_content = AsyncMock(return_value="Test content $19.99")
    locator.get_attribute = AsyncMock(return_value="/dispensaries/test-store-fl")
    locator.inner_text = AsyncMock(return_value="Test Store - Miami, FL $25.99")
    locator.is_visible = AsyncMock(return_value=True)
    locator.click = AsyncMock()
    locator.first = Mock(return_value=locator)
    locator.nth = Mock(return_value=locator)
    locator.locator = Mock(return_value=locator)
    
    return locator


@pytest.fixture
def mock_playwright_context():
    """Mock Playwright browser context for testing."""
    context = AsyncMock()
    context.new_page = AsyncMock(return_value=mock_playwright_page())
    context.close = AsyncMock()
    return context


@pytest.fixture
def florida_store_test_data():
    """Test data for Florida store detection."""
    return [
        # Should match Florida stores
        ("Test Store - Miami, FL", "/dispensaries/miami-fl", True),
        ("Orlando Dispensary, FL", "/dispensaries/orlando-florida", True), 
        ("Tampa Store FL", "/dispensaries/tampa-fl-store", True),
        ("Jacksonville - FL Location", "/dispensaries/jacksonville", True),
        
        # Should not match non-Florida stores
        ("California Store", "/dispensaries/los-angeles-ca", False),
        ("New York Location", "/dispensaries/new-york-ny", False),
        ("Texas Dispensary", "/dispensaries/houston-tx", False),
        ("Generic Store", "/dispensaries/generic-store", False),
    ]


@pytest.fixture
def price_extraction_test_data():
    """Test data for price extraction patterns."""
    return [
        # Standard price formats
        ("$25.99", 25.99),
        ("$ 19.50", 19.50),
        ("Price: $45.00", 45.00),
        ("$12", 12.00),
        ("Only $99.95 today!", 99.95),
        
        # Multiple prices - should return minimum
        ("$25.99 or $19.99 on sale", 19.99),
        ("Regular $45.00, Sale $35.00", 35.00),
        
        # Edge cases
        ("No price here", None),
        ("", None),
        ("$0.00", 0.00),
        ("Free item", None),
    ]


@pytest.fixture
def size_standardization_test_data():
    """Test data for size standardization."""
    return [
        # Standard size formats
        ("3.5g", 3.5),
        ("1G", 1.0),
        ("7g flower", 7.0),
        ("14g package", 14.0),
        ("0.5g pre-roll", 0.5),
        ("28g bulk", 28.0),
        
        # Invalid or missing sizes
        ("no size", None),
        ("", None),
        ("5g", None),  # Not in SIZE_MAP
        ("invalid", None),
    ]


@pytest.fixture
def thc_extraction_test_data():
    """Test data for THC extraction patterns."""
    return [
        # Single THC percentages
        ("THC: 22.5%", 22.5),
        ("THC 18%", 18.0),
        ("Contains 25.7% THC", 25.7),
        
        # THC ranges - should return first value
        ("THC 18%-22%", 18.0),
        ("THC: 15.5% - 18.2%", 15.5),
        
        # No THC information
        ("No THC listed", None),
        ("", None),
        ("CBD only product", None),
    ]


@pytest.fixture
def deduplication_test_data():
    """Test data for deduplication logic."""
    base_product = {
        "state": "FL",
        "store": "Test Store",
        "subcategory": "Whole Flower",
        "name": "Blue Dream",
        "size_raw": "3.5g",
        "grams": 3.5,
        "price": 45.00
    }
    
    # Create duplicates and unique products
    products = []
    
    # Exact duplicate
    products.append(base_product.copy())
    products.append(base_product.copy())
    
    # Different price (same dedup key)
    duplicate_diff_price = base_product.copy()
    duplicate_diff_price["price"] = 50.00
    products.append(duplicate_diff_price)
    
    # Different size (different dedup key)
    unique_diff_size = base_product.copy()
    unique_diff_size["size_raw"] = "7g"
    unique_diff_size["grams"] = 7.0
    products.append(unique_diff_size)
    
    # Different store (different dedup key)  
    unique_diff_store = base_product.copy()
    unique_diff_store["store"] = "Different Store"
    products.append(unique_diff_store)
    
    return products


@pytest.fixture
def scraping_result_sample():
    """Sample scraping result for testing."""
    products = [sample_product_data()]
    return ScrapingResult(
        category="Whole Flower",
        products=products,
        store_count=1,
        total_products=len(products),
        success=True,
        duration_seconds=45.5,
        scraped_at=datetime.now()
    )


# Test helper functions
def create_mock_agent_with_test_model():
    """Create agent instance with TestModel for testing."""
    from pydantic_ai.models.test import TestModel
    from ..agent import dispensary_agent
    
    # Override the agent model with TestModel
    return dispensary_agent.override(model=TestModel())


async def run_agent_test(agent, prompt: str, deps=None):
    """Helper to run agent tests with proper error handling."""
    try:
        result = await agent.run(prompt, deps=deps)
        return result.data
    except Exception as e:
        pytest.fail(f"Agent test failed: {e}")


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"  
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )