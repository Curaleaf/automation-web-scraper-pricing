"""
Dispensary Web Scraping Automation Framework

A Pydantic AI agent-based automation framework for dispensary pricing data extraction
with parallel processing, Snowflake integration, and GitHub Actions deployment.
"""

from .agent import dispensary_agent
from .models import ProductData, ScrapingResult, SnowflakeConfig

__version__ = "0.1.0"
__all__ = ["dispensary_agent", "ProductData", "ScrapingResult", "SnowflakeConfig"]