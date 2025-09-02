"""
Configuration management using pydantic-settings with python-dotenv.

CRITICAL: Uses python-dotenv exactly as shown in main_agent_reference/settings.py
Load environment variables from .env file before Settings class instantiation.
"""

import os
from typing import Optional, Dict, List
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ConfigDict
from dotenv import load_dotenv

# CRITICAL: Must be called before Settings class instantiation
load_dotenv()


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    
    Follows the exact pattern from main_agent_reference/settings.py with
    additional Snowflake and Playwright configuration for dispensary scraping.
    """
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # LLM Configuration (following reference pattern)
    llm_provider: str = Field(default="openai", description="LLM provider")
    llm_api_key: str = Field(..., description="API key for LLM provider")
    llm_model: str = Field(default="gpt-4", description="Model name to use")
    llm_base_url: str = Field(
        default="https://api.openai.com/v1",
        description="Base URL for LLM API"
    )
    
    # Snowflake Configuration
    snowflake_account: str = Field(
        default="CURALEAF-CURAPROD.snowflakecomputing.com",
        description="Snowflake account identifier"
    )
    snowflake_user: str = Field(..., description="Snowflake username")
    snowflake_password: str = Field(..., description="Snowflake password")
    snowflake_database: str = Field(
        default="SANDBOX_EDW",
        description="Snowflake database name"
    )
    snowflake_schema: str = Field(
        default="ANALYTICS", 
        description="Snowflake schema name"
    )
    snowflake_warehouse: str = Field(
        default="COMPUTE_WH",
        description="Snowflake warehouse name"
    )
    snowflake_role: Optional[str] = Field(
        default=None,
        description="Snowflake role to use"
    )
    
    # Snowflake Table Mapping (CRITICAL: Must match existing table names exactly)
    snowflake_tables: Dict[str, str] = Field(
        default={
            "Whole Flower": "TL_Scrape_WHOLE_FLOWER",
            "Pre-Rolls": "TL_Scrape_Pre_Rolls",
            "Ground & Shake": "TL_Scrape_Ground_Shake"
        },
        description="Mapping of subcategories to Snowflake table names"
    )
    
    # Scraping Configuration
    scraping_categories: Dict[str, str] = Field(
        default={
            "Whole Flower": "https://www.trulieve.com/category/flower/whole-flower",
            "Pre-Rolls": "https://www.trulieve.com/category/flower/pre-rolls",
            "Ground & Shake": "https://www.trulieve.com/category/flower/ground-shake"
        },
        description="Category URLs to scrape"
    )
    
    # Rate Limiting Configuration (CRITICAL: From proven notebook patterns)
    min_delay_ms: int = Field(
        default=700,
        description="Minimum delay between requests (proven from notebook)"
    )
    max_delay_ms: int = Field(
        default=1500,
        description="Maximum delay between requests (proven from notebook)"
    )
    page_timeout_ms: int = Field(
        default=20000,
        description="Page load timeout in milliseconds"
    )
    
    # Retry Configuration
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for failed operations"
    )
    retry_delay_seconds: int = Field(
        default=2,
        description="Base delay between retries"
    )
    
    # Playwright Configuration
    playwright_headless: bool = Field(
        default=True,
        description="Run browser in headless mode"
    )
    playwright_user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        description="User agent string (matches proven notebook pattern)"
    )
    playwright_browser_args: List[str] = Field(
        default=["--no-sandbox"],
        description="Browser launch arguments (required for server environments)"
    )
    
    # Parallel Processing Configuration
    max_concurrent_stores: int = Field(
        default=5,
        description="Maximum stores to scrape concurrently"
    )
    max_concurrent_categories: int = Field(
        default=3,
        description="Maximum categories to scrape concurrently"
    )
    
    # Data Validation Configuration
    min_expected_products: int = Field(
        default=100,
        description="Minimum expected products per category"
    )
    min_expected_stores: int = Field(
        default=50,
        description="Minimum expected Florida stores"
    )
    
    # Application Configuration (following reference pattern)
    app_env: str = Field(
        default="development",
        description="Application environment"
    )
    log_level: str = Field(
        default="INFO", 
        description="Logging level"
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode"
    )
    
    # Session Configuration
    session_id: Optional[str] = Field(
        default=None,
        description="Optional session identifier"
    )
    
    @field_validator("llm_api_key", "snowflake_user", "snowflake_password")
    @classmethod
    def validate_required_secrets(cls, v):
        """Ensure required secret fields are not empty (following reference pattern)."""
        if not v or v.strip() == "":
            raise ValueError("Required secret cannot be empty")
        return v
    
    @field_validator("snowflake_account", "snowflake_database", "snowflake_schema", "snowflake_warehouse")
    @classmethod
    def validate_snowflake_config(cls, v):
        """Ensure Snowflake configuration fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Snowflake configuration field cannot be empty")
        return v.strip()
    
    @field_validator("min_delay_ms", "max_delay_ms", "page_timeout_ms")
    @classmethod
    def validate_timing_config(cls, v):
        """Validate timing configuration is positive."""
        if v <= 0:
            raise ValueError("Timing values must be positive")
        return v
    
    @field_validator("max_retries")
    @classmethod
    def validate_retries(cls, v):
        """Validate retry count is reasonable."""
        if v < 0 or v > 10:
            raise ValueError("Max retries must be between 0 and 10")
        return v
    
    @field_validator("max_concurrent_stores", "max_concurrent_categories")
    @classmethod
    def validate_concurrency(cls, v):
        """Validate concurrency limits are reasonable."""
        if v < 1 or v > 20:
            raise ValueError("Concurrency limits must be between 1 and 20")
        return v
    
    def to_snowflake_config(self):
        """
        Convert to SnowflakeConfig model.
        
        Returns:
            SnowflakeConfig: Snowflake configuration object
        """
        from .models import SnowflakeConfig
        
        return SnowflakeConfig(
            account=self.snowflake_account,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            user=self.snowflake_user,
            password=self.snowflake_password,
            warehouse=self.snowflake_warehouse,
            role=self.snowflake_role,
            tables=self.snowflake_tables
        )
    
    def to_agent_config(self):
        """
        Convert to AgentConfig model.
        
        Returns:
            AgentConfig: Agent configuration object
        """
        from .models import AgentConfig
        
        return AgentConfig(
            categories=self.scraping_categories,
            min_delay_ms=self.min_delay_ms,
            max_delay_ms=self.max_delay_ms,
            page_timeout_ms=self.page_timeout_ms,
            max_retries=self.max_retries,
            retry_delay_seconds=self.retry_delay_seconds,
            headless=self.playwright_headless,
            user_agent=self.playwright_user_agent,
            browser_args=self.playwright_browser_args,
            max_concurrent_stores=self.max_concurrent_stores,
            max_concurrent_categories=self.max_concurrent_categories,
            min_expected_products=self.min_expected_products,
            min_expected_stores=self.min_expected_stores
        )


def load_settings() -> Settings:
    """
    Load settings with proper error handling and environment loading.
    
    Returns:
        Settings: Configured settings object
        
    Raises:
        ValueError: If required configuration is missing or invalid
    """
    try:
        return Settings()
    except Exception as e:
        error_msg = f"Failed to load settings: {e}"
        
        # Provide helpful error messages for common issues
        if "llm_api_key" in str(e).lower():
            error_msg += "\nMake sure to set LLM_API_KEY in your .env file"
        if "snowflake_user" in str(e).lower() or "snowflake_password" in str(e).lower():
            error_msg += "\nMake sure to set SNOWFLAKE_USER and SNOWFLAKE_PASSWORD in your .env file"
        
        raise ValueError(error_msg) from e


# Global settings instance (following reference pattern)
try:
    settings = load_settings()
except Exception:
    # For testing, create settings with dummy values (following reference pattern)
    import os
    os.environ.setdefault("LLM_API_KEY", "test_key")
    os.environ.setdefault("SNOWFLAKE_USER", "test_user")
    os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
    settings = Settings()