"""
Dependencies for the dispensary scraper agent.

CRITICAL: This follows the exact pattern from the PRP for Pydantic AI agent 
dependency injection with Playwright context and Snowflake client management.
"""

import asyncio
import sys
from dataclasses import dataclass
from typing import Any, Optional
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright, BrowserContext
from .models import SnowflakeConfig


@dataclass
class ScrapingDependencies:
    """
    Dependencies for agent tools.
    
    CRITICAL: Pydantic AI agents need deps_type for external dependencies.
    This provides dependency injection for Playwright context and Snowflake client.
    """
    playwright_context: BrowserContext
    snowflake_client: Any  # Will be SnowflakeClient once implemented
    session_id: Optional[str] = None


class DependencyManager:
    """
    Manages the lifecycle of external dependencies.
    
    Handles creation, configuration, and cleanup of Playwright browser contexts
    and Snowflake database connections with proper error handling.
    """
    
    def __init__(self, snowflake_config: SnowflakeConfig, headless: bool = True):
        """
        Initialize dependency manager.
        
        Args:
            snowflake_config: Snowflake connection configuration
            headless: Whether to run browser in headless mode
        """
        self.snowflake_config = snowflake_config
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.snowflake_client = None
    
    async def setup_playwright(self) -> BrowserContext:
        """
        Setup Playwright browser context with proper configuration.
        
        CRITICAL: Windows requires specific async setup and browser args from notebook.
        
        Returns:
            Configured browser context
        """
        # CRITICAL: Playwright requires specific async setup on Windows  
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        self.playwright = await async_playwright().start()
        
        # Launch browser with proven args from notebook
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox"]  # CRITICAL: Required for server environments
        )
        
        # Create context with custom user agent matching notebook
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
        )
        
        return self.context
    
    async def setup_snowflake_client(self):
        """
        Setup Snowflake client with connection pooling and retry logic.
        
        Will import and create SnowflakeClient once implemented.
        """
        # Import here to avoid circular imports
        from .snowflake_client import SnowflakeClient
        
        self.snowflake_client = SnowflakeClient(self.snowflake_config)
        await self.snowflake_client.connect()
        return self.snowflake_client
    
    async def create_dependencies(self) -> ScrapingDependencies:
        """
        Create and configure all dependencies.
        
        Returns:
            Configured ScrapingDependencies object
        """
        playwright_context = await self.setup_playwright()
        
        # Setup Snowflake client (will be implemented)
        try:
            snowflake_client = await self.setup_snowflake_client()
        except ImportError:
            # Placeholder until SnowflakeClient is implemented
            snowflake_client = None
        
        return ScrapingDependencies(
            playwright_context=playwright_context,
            snowflake_client=snowflake_client,
            session_id=None  # Can be set later if needed
        )
    
    async def cleanup(self):
        """
        Clean up all dependencies and connections.
        
        CRITICAL: Proper cleanup prevents resource leaks and browser processes.
        """
        errors = []
        
        # Cleanup Snowflake connection
        if self.snowflake_client:
            try:
                await self.snowflake_client.close()
            except Exception as e:
                errors.append(f"Snowflake cleanup error: {e}")
        
        # Cleanup Playwright resources
        if self.context:
            try:
                await self.context.close()
            except Exception as e:
                errors.append(f"Browser context cleanup error: {e}")
        
        if self.browser:
            try:
                await self.browser.close()
            except Exception as e:
                errors.append(f"Browser cleanup error: {e}")
        
        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception as e:
                errors.append(f"Playwright cleanup error: {e}")
        
        if errors:
            # Log errors but don't raise - cleanup should be best effort
            print(f"Cleanup errors: {'; '.join(errors)}")


@asynccontextmanager
async def create_scraping_dependencies(
    snowflake_config: SnowflakeConfig,
    headless: bool = True,
    session_id: Optional[str] = None
):
    """
    Context manager for creating and cleaning up scraping dependencies.
    
    Usage:
        async with create_scraping_dependencies(snowflake_config) as deps:
            # Use deps with Pydantic AI agent
            result = await agent.run("scrape all categories", deps=deps)
    
    Args:
        snowflake_config: Snowflake connection configuration
        headless: Whether to run browser in headless mode
        session_id: Optional session identifier
        
    Yields:
        ScrapingDependencies: Configured dependencies
    """
    manager = DependencyManager(snowflake_config, headless)
    
    try:
        # Create and configure dependencies
        deps = await manager.create_dependencies()
        
        # Set session ID if provided
        if session_id:
            deps.session_id = session_id
        
        yield deps
    
    finally:
        # Always cleanup, even if errors occur
        await manager.cleanup()


class MockScrapingDependencies:
    """
    Mock dependencies for testing.
    
    Provides test doubles for Playwright context and Snowflake client
    to enable unit testing without external dependencies.
    """
    
    def __init__(self, session_id: Optional[str] = None):
        """Initialize with mock objects."""
        self.playwright_context = MockBrowserContext()
        self.snowflake_client = MockSnowflakeClient()
        self.session_id = session_id or "test-session"


class MockBrowserContext:
    """Mock Playwright browser context for testing."""
    
    async def new_page(self):
        """Return mock page."""
        return MockPage()
    
    async def close(self):
        """Mock close method."""
        pass


class MockPage:
    """Mock Playwright page for testing."""
    
    async def goto(self, url: str, **kwargs):
        """Mock navigation."""
        pass
    
    async def locator(self, selector: str):
        """Mock locator."""
        return MockLocator()
    
    async def close(self):
        """Mock close."""
        pass
    
    async def wait_for_timeout(self, ms: int):
        """Mock wait - no actual delay in tests."""
        pass
    
    def get_by_role(self, role: str, **kwargs):
        """Mock role selector."""
        return MockLocator()
    
    @property
    def mouse(self):
        """Mock mouse."""
        return MockMouse()


class MockLocator:
    """Mock Playwright locator for testing."""
    
    async def all(self):
        """Return empty list."""
        return []
    
    async def count(self):
        """Return 0."""
        return 0
    
    async def text_content(self):
        """Return empty string."""
        return ""
    
    async def get_attribute(self, name: str):
        """Return None.""" 
        return None
    
    async def inner_text(self):
        """Return empty string."""
        return ""
    
    def locator(self, selector: str):
        """Return self."""
        return self
    
    async def is_visible(self):
        """Return False."""
        return False
    
    async def click(self):
        """Mock click."""
        pass
    
    def first(self):
        """Return self."""
        return self
    
    def nth(self, index: int):
        """Return self."""
        return self


class MockMouse:
    """Mock Playwright mouse for testing."""
    
    async def wheel(self, delta_x: int, delta_y: int):
        """Mock scroll."""
        pass


class MockSnowflakeClient:
    """Mock Snowflake client for testing."""
    
    async def connect(self):
        """Mock connect."""
        pass
    
    async def insert_products(self, products, table_name: str):
        """Mock insert."""
        return True
    
    async def close(self):
        """Mock close."""
        pass