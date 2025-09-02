"""
Pydantic models for dispensary scraping automation.

These models mirror the existing notebook data structure exactly to ensure
compatibility with existing analytics and reporting systems.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class ProductData(BaseModel):
    """
    Product data model matching existing notebook output structure.
    
    This model mirrors the exact structure from the working notebook to ensure
    compatibility with existing Snowflake tables and analytics systems.
    """
    state: str = Field(default="FL", description="State abbreviation")
    store: str = Field(..., description="Store name")
    subcategory: str = Field(..., description="Product subcategory (Whole Flower, Pre-Rolls, Ground & Shake)")
    name: str = Field(..., description="Product name")
    brand: Optional[str] = Field(None, description="Brand name")
    strain_type: Optional[str] = Field(None, description="Strain type: Indica, Sativa, or Hybrid")
    thc_pct: Optional[float] = Field(None, description="THC percentage")
    size_raw: Optional[str] = Field(None, description="Raw size string (e.g., '3.5g')")
    grams: Optional[float] = Field(None, description="Size in grams")
    price: Optional[float] = Field(None, description="Product price in USD")
    price_per_g: Optional[float] = Field(None, description="Price per gram in USD")
    url: Optional[str] = Field(None, description="Product URL")
    scraped_at: datetime = Field(default_factory=datetime.now, description="Timestamp when data was scraped")
    
    @field_validator("strain_type")
    @classmethod
    def validate_strain_type(cls, v):
        """Validate strain type is one of the expected values."""
        if v is not None and v not in ["Indica", "Sativa", "Hybrid"]:
            return None  # Return None for invalid strain types rather than raising error
        return v
    
    @field_validator("subcategory")
    @classmethod
    def validate_subcategory(cls, v):
        """Validate subcategory is one of the expected values."""
        valid_categories = ["Whole Flower", "Pre-Rolls", "Ground & Shake"]
        if v not in valid_categories:
            raise ValueError(f"Subcategory must be one of: {valid_categories}")
        return v
    
    @field_validator("thc_pct")
    @classmethod
    def validate_thc_pct(cls, v):
        """Validate THC percentage is reasonable."""
        if v is not None and (v < 0 or v > 100):
            return None  # Return None for invalid THC percentages
        return v
    
    @field_validator("price", "price_per_g")
    @classmethod
    def validate_prices(cls, v):
        """Validate prices are positive."""
        if v is not None and v < 0:
            return None  # Return None for negative prices
        return v
    
    @field_validator("grams")
    @classmethod
    def validate_grams(cls, v):
        """Validate grams is positive."""
        if v is not None and v <= 0:
            return None  # Return None for invalid weights
        return v


class ScrapingResult(BaseModel):
    """
    Result from a category scraping operation.
    
    Used for tracking the success and results of individual scraping operations
    and aggregating results from parallel scraping tasks.
    """
    category: str = Field(..., description="Category that was scraped")
    products: List[ProductData] = Field(default_factory=list, description="List of scraped products")
    store_count: int = Field(default=0, description="Number of stores scraped")
    total_products: int = Field(default=0, description="Total number of products found")
    success: bool = Field(default=True, description="Whether scraping was successful")
    error_message: Optional[str] = Field(None, description="Error message if scraping failed")
    duration_seconds: Optional[float] = Field(None, description="Time taken to complete scraping")
    scraped_at: datetime = Field(default_factory=datetime.now, description="When scraping completed")
    
    def __post_init__(self):
        """Update computed fields after initialization."""
        if not self.total_products:
            self.total_products = len(self.products)


class SnowflakeConfig(BaseModel):
    """
    Snowflake database configuration.
    
    Configuration for connecting to and inserting data into Snowflake tables.
    Maintains existing table schema and names exactly as specified in PRP.
    """
    account: str = Field(default="CURALEAF-CURAPROD.snowflakecomputing.com", description="Snowflake account")
    database: str = Field(default="SANDBOX_EDW", description="Database name")
    schema: str = Field(default="ANALYTICS", description="Schema name")
    user: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    warehouse: str = Field(default="COMPUTE_WH", description="Warehouse name")
    role: Optional[str] = Field(None, description="Role to use")
    
    # CRITICAL: Table mapping must match existing Snowflake table names exactly
    tables: Dict[str, str] = Field(
        default={
            "Whole Flower": "TL_Scrape_WHOLE_FLOWER",
            "Pre-Rolls": "TL_Scrape_Pre_Rolls", 
            "Ground & Shake": "TL_Scrape_Ground_Shake"
        },
        description="Mapping of subcategories to Snowflake table names"
    )
    
    @field_validator("account", "database", "schema", "user", "password", "warehouse")
    @classmethod
    def validate_required_fields(cls, v):
        """Ensure required fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()


class ScrapingSession(BaseModel):
    """
    Complete scraping session results.
    
    Aggregates results from all categories scraped in a single session.
    """
    session_id: str = Field(..., description="Unique session identifier")
    results: Dict[str, ScrapingResult] = Field(default_factory=dict, description="Results by category")
    total_products: int = Field(default=0, description="Total products across all categories")
    total_stores: int = Field(default=0, description="Total stores scraped")
    success: bool = Field(default=True, description="Whether entire session was successful")
    start_time: datetime = Field(default_factory=datetime.now, description="When session started")
    end_time: Optional[datetime] = Field(None, description="When session completed")
    duration_seconds: Optional[float] = Field(None, description="Total session duration")
    errors: List[str] = Field(default_factory=list, description="List of error messages")
    
    def add_result(self, category: str, result: ScrapingResult):
        """Add a category result to the session."""
        self.results[category] = result
        self.total_products += result.total_products
        if not result.success:
            self.success = False
            if result.error_message:
                self.errors.append(f"{category}: {result.error_message}")
    
    def finalize(self):
        """Finalize the session by setting end time and calculating duration."""
        self.end_time = datetime.now()
        if self.start_time and self.end_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        # Calculate unique store count across all categories
        all_stores = set()
        for result in self.results.values():
            for product in result.products:
                all_stores.add(product.store)
        self.total_stores = len(all_stores)


class DatabaseInsertResult(BaseModel):
    """
    Result from database insertion operation.
    
    Tracks the success and details of inserting scraped data into Snowflake.
    """
    table_name: str = Field(..., description="Target table name")
    category: str = Field(..., description="Product category")
    success: bool = Field(..., description="Whether insertion was successful")
    rows_inserted: int = Field(default=0, description="Number of rows successfully inserted")
    rows_failed: int = Field(default=0, description="Number of rows that failed to insert")
    error_message: Optional[str] = Field(None, description="Error message if insertion failed")
    duration_seconds: Optional[float] = Field(None, description="Time taken for insertion")
    inserted_at: datetime = Field(default_factory=datetime.now, description="When insertion completed")


class AgentConfig(BaseModel):
    """
    Configuration for the dispensary scraper agent.
    
    Central configuration object that combines all settings needed for the agent
    to operate including Playwright settings, rate limiting, and retry logic.
    """
    # Scraping configuration
    categories: Dict[str, str] = Field(
        default={
            "Whole Flower": "https://www.trulieve.com/category/flower/whole-flower",
            "Pre-Rolls": "https://www.trulieve.com/category/flower/pre-rolls", 
            "Ground & Shake": "https://www.trulieve.com/category/flower/ground-shake"
        },
        description="Category URLs to scrape"
    )
    
    # Rate limiting and timeouts
    min_delay_ms: int = Field(default=700, description="Minimum delay between requests in milliseconds")
    max_delay_ms: int = Field(default=1500, description="Maximum delay between requests in milliseconds") 
    page_timeout_ms: int = Field(default=20000, description="Page load timeout in milliseconds")
    
    # Retry configuration  
    max_retries: int = Field(default=3, description="Maximum number of retries for failed operations")
    retry_delay_seconds: int = Field(default=2, description="Base delay between retries in seconds")
    
    # Playwright configuration
    headless: bool = Field(default=True, description="Run browser in headless mode")
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
        description="User agent string for browser"
    )
    browser_args: List[str] = Field(
        default=["--no-sandbox"],
        description="Additional browser launch arguments"
    )
    
    # Parallel processing
    max_concurrent_stores: int = Field(default=5, description="Maximum number of stores to scrape concurrently")
    max_concurrent_categories: int = Field(default=3, description="Maximum number of categories to scrape concurrently")
    
    # Data validation
    min_expected_products: int = Field(default=100, description="Minimum expected products per category")
    min_expected_stores: int = Field(default=50, description="Minimum expected Florida stores")
    
    @field_validator("min_delay_ms", "max_delay_ms", "page_timeout_ms")
    @classmethod
    def validate_positive_timing(cls, v):
        """Validate timing values are positive."""
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