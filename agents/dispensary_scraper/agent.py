"""
Dispensary Scraper Agent - Main orchestration agent for parallel dispensary scraping.

CRITICAL: Mirrors pattern from main_agent_reference/research_agent.py with 
parallel execution of 3 categories while maintaining proper rate limiting.
"""

import asyncio
import logging
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime

from pydantic_ai import Agent, RunContext

from .providers import get_llm_model
from .dependencies import ScrapingDependencies
from .models import (
    ProductData, ScrapingResult, ScrapingSession, DatabaseInsertResult
)
from .tools import (
    extract_florida_stores_tool, 
    scrape_store_category_tool
)

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are an expert dispensary data scraping orchestrator. Your primary goal is to coordinate 
parallel web scraping operations across multiple dispensary categories and manage the data 
pipeline into Snowflake database.

Your capabilities:
1. **Parallel Scraping**: Orchestrate simultaneous scraping of multiple product categories
2. **Store Discovery**: Find and validate Florida dispensary locations  
3. **Data Management**: Structure and validate scraped product data
4. **Database Integration**: Insert results into appropriate Snowflake tables
5. **Error Handling**: Manage failures and retry operations gracefully

Core Categories to Scrape:
- Whole Flower: Cannabis flower products
- Pre-Rolls: Pre-rolled cannabis products  
- Ground & Shake: Ground cannabis products

When conducting scraping operations:
- Always respect rate limits (700-1500ms delays between requests)
- Use proven regex patterns for price, size, and THC extraction
- Maintain Florida store filtering to ensure data quality
- Implement proper deduplication using (store, slug, size, category) keys
- Handle errors gracefully and provide detailed status reports

Always strive to provide comprehensive, accurate dispensary pricing data while 
maintaining respectful scraping practices and data quality standards.
"""


# CRITICAL: Pydantic AI agents need deps_type for external dependencies
dispensary_agent = Agent(
    get_llm_model(),
    deps_type=ScrapingDependencies,
    system_prompt=SYSTEM_PROMPT
)


@dispensary_agent.tool
async def get_florida_stores(
    ctx: RunContext[ScrapingDependencies]
) -> List[Dict[str, str]]:
    """
    Get list of Florida dispensary stores.
    
    Returns:
        List of store dictionaries with name and URL
    """
    try:
        stores = await extract_florida_stores_tool(ctx)
        logger.info(f"Found {len(stores)} Florida stores")
        return [{"name": name, "url": url} for name, url in stores]
    except Exception as e:
        logger.error(f"Failed to get Florida stores: {e}")
        return []


@dispensary_agent.tool
async def scrape_single_category(
    ctx: RunContext[ScrapingDependencies],
    category_name: str,
    category_url: str,
    max_stores: Optional[int] = None
) -> ScrapingResult:
    """
    Scrape a single product category across all Florida stores.
    
    Args:
        category_name: Name of the category (e.g., "Whole Flower")
        category_url: URL for the category page
        max_stores: Optional limit on number of stores (for testing)
        
    Returns:
        ScrapingResult with aggregated data from all stores
    """
    start_time = datetime.now()
    
    try:
        # Get Florida stores
        stores = await extract_florida_stores_tool(ctx)
        if max_stores:
            stores = stores[:max_stores]
        
        logger.info(f"Scraping {category_name} across {len(stores)} stores")
        
        all_products = []
        successful_stores = 0
        
        # CRITICAL: Maintain rate limiting per store while processing sequentially
        for store_name, store_url in stores:
            try:
                products = await scrape_store_category_tool(
                    ctx, store_name, store_url, category_url, category_name
                )
                all_products.extend(products)
                successful_stores += 1
                logger.debug(f"Scraped {len(products)} products from {store_name}")
                
            except Exception as e:
                logger.warning(f"Failed to scrape {store_name}: {e}")
                continue
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = ScrapingResult(
            category=category_name,
            products=all_products,
            store_count=successful_stores,
            total_products=len(all_products),
            success=len(all_products) > 0,
            duration_seconds=duration,
            scraped_at=end_time
        )
        
        if not result.success:
            result.error_message = f"No products found for {category_name}"
        
        logger.info(
            f"Completed {category_name}: {len(all_products)} products "
            f"from {successful_stores} stores in {duration:.1f}s"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error scraping {category_name}: {e}")
        return ScrapingResult(
            category=category_name,
            success=False,
            error_message=str(e),
            scraped_at=datetime.now()
        )


@dispensary_agent.tool
async def scrape_all_categories(
    ctx: RunContext[ScrapingDependencies],
    max_stores_per_category: Optional[int] = None,
    categories_to_scrape: Optional[List[str]] = None
) -> Dict[str, ScrapingResult]:
    """
    Scrape all dispensary categories in parallel.
    
    CRITICAL: Implements parallel execution pattern from PRP pseudocode.
    
    Args:
        max_stores_per_category: Optional limit on stores per category (for testing)
        categories_to_scrape: Optional list to limit which categories to scrape
        
    Returns:
        Dictionary mapping category names to ScrapingResult objects
    """
    # PATTERN: Concurrent execution of multiple categories (from PRP)
    default_categories = [
        ("https://www.trulieve.com/category/flower/whole-flower", "Whole Flower"),
        ("https://www.trulieve.com/category/flower/pre-rolls", "Pre-Rolls"),
        ("https://www.trulieve.com/category/flower/ground-shake", "Ground & Shake")
    ]
    
    # Filter categories if specified
    if categories_to_scrape:
        categories = [
            (url, name) for url, name in default_categories
            if name in categories_to_scrape
        ]
    else:
        categories = default_categories
    
    logger.info(f"Starting parallel scraping of {len(categories)} categories")
    
    # Create tasks for parallel execution
    tasks = []
    for category_url, subcategory in categories:
        task = scrape_single_category(
            ctx, subcategory, category_url, max_stores_per_category
        )
        tasks.append((subcategory, task))
    
    # CRITICAL: Parallel execution while maintaining rate limiting per category
    results = {}
    completed_tasks = await asyncio.gather(
        *[task for _, task in tasks], 
        return_exceptions=True
    )
    
    # Process results
    for i, result in enumerate(completed_tasks):
        category_name = tasks[i][0]
        
        if isinstance(result, Exception):
            logger.error(f"Category {category_name} failed with exception: {result}")
            results[category_name] = ScrapingResult(
                category=category_name,
                success=False,
                error_message=str(result),
                scraped_at=datetime.now()
            )
        else:
            results[category_name] = result
    
    # Log summary
    total_products = sum(r.total_products for r in results.values())
    successful_categories = sum(1 for r in results.values() if r.success)
    
    logger.info(
        f"Parallel scraping completed: {successful_categories}/{len(categories)} "
        f"categories successful, {total_products} total products"
    )
    
    return results


@dispensary_agent.tool
async def store_results_in_snowflake(
    ctx: RunContext[ScrapingDependencies],
    scraping_results: Dict[str, ScrapingResult]
) -> Dict[str, DatabaseInsertResult]:
    """
    Store scraping results in Snowflake database.
    
    Args:
        scraping_results: Dictionary of category results to store
        
    Returns:
        Dictionary mapping categories to database insert results
    """
    if not ctx.deps.snowflake_client:
        logger.error("Snowflake client not available")
        return {}
    
    logger.info("Storing results in Snowflake database")
    
    # Organize products by category
    products_by_category = {}
    for category, result in scraping_results.items():
        if result.success and result.products:
            products_by_category[category] = result.products
    
    if not products_by_category:
        logger.warning("No successful scraping results to store")
        return {}
    
    # Insert products by category
    try:
        insert_results = await ctx.deps.snowflake_client.insert_products_by_category(
            products_by_category
        )
        
        # Log results
        for category, insert_result in insert_results.items():
            if insert_result.success:
                logger.info(
                    f"Successfully inserted {insert_result.rows_inserted} "
                    f"{category} products into {insert_result.table_name}"
                )
            else:
                logger.error(
                    f"Failed to insert {category} products: "
                    f"{insert_result.error_message}"
                )
        
        return insert_results
        
    except Exception as e:
        logger.error(f"Error storing results in Snowflake: {e}")
        return {
            category: DatabaseInsertResult(
                table_name="unknown",
                category=category,
                success=False,
                error_message=str(e)
            )
            for category in products_by_category.keys()
        }


@dispensary_agent.tool
async def run_complete_scraping_workflow(
    ctx: RunContext[ScrapingDependencies],
    max_stores_per_category: Optional[int] = None,
    categories_to_scrape: Optional[List[str]] = None,
    store_in_database: bool = True
) -> ScrapingSession:
    """
    Run the complete dispensary scraping workflow.
    
    This is the main orchestration function that:
    1. Scrapes all categories in parallel
    2. Stores results in Snowflake (if enabled)
    3. Returns comprehensive session results
    
    Args:
        max_stores_per_category: Optional limit on stores per category
        categories_to_scrape: Optional list to limit which categories to scrape
        store_in_database: Whether to store results in Snowflake
        
    Returns:
        ScrapingSession with complete workflow results
    """
    session_id = str(uuid.uuid4())
    session = ScrapingSession(
        session_id=session_id,
        start_time=datetime.now()
    )
    
    logger.info(f"Starting complete scraping workflow [Session: {session_id}]")
    
    try:
        # Phase 1: Parallel scraping of all categories
        logger.info("Phase 1: Scraping all categories in parallel")
        scraping_results = await scrape_all_categories(
            ctx, max_stores_per_category, categories_to_scrape
        )
        
        # Add results to session
        for category, result in scraping_results.items():
            session.add_result(category, result)
        
        # Phase 2: Store results in Snowflake (if enabled)
        if store_in_database and ctx.deps.snowflake_client:
            logger.info("Phase 2: Storing results in Snowflake")
            database_results = await store_results_in_snowflake(ctx, scraping_results)
            
            # Check for database errors
            for category, db_result in database_results.items():
                if not db_result.success:
                    session.errors.append(
                        f"Database insert failed for {category}: {db_result.error_message}"
                    )
        elif store_in_database:
            logger.warning("Database storage requested but Snowflake client not available")
            session.errors.append("Snowflake client not available for database storage")
        
        # Finalize session
        session.finalize()
        
        logger.info(
            f"Workflow completed [Session: {session_id}]: "
            f"{session.total_products} products from {session.total_stores} stores "
            f"in {session.duration_seconds:.1f}s"
        )
        
        return session
        
    except Exception as e:
        logger.error(f"Workflow failed [Session: {session_id}]: {e}")
        session.success = False
        session.errors.append(str(e))
        session.finalize()
        return session


# Convenience function for direct usage
async def run_dispensary_scraping(
    snowflake_config,
    max_stores_per_category: Optional[int] = None,
    categories_to_scrape: Optional[List[str]] = None,
    store_in_database: bool = True,
    headless: bool = True
) -> ScrapingSession:
    """
    Convenience function to run dispensary scraping with automatic dependency management.
    
    Args:
        snowflake_config: Snowflake configuration
        max_stores_per_category: Optional limit on stores per category  
        categories_to_scrape: Optional list to limit which categories to scrape
        store_in_database: Whether to store results in Snowflake
        headless: Whether to run browser in headless mode
        
    Returns:
        ScrapingSession with complete results
    """
    from .dependencies import create_scraping_dependencies
    
    async with create_scraping_dependencies(
        snowflake_config=snowflake_config,
        headless=headless
    ) as deps:
        
        result = await dispensary_agent.run(
            f"""Run the complete dispensary scraping workflow.
            
            Configuration:
            - Max stores per category: {max_stores_per_category or 'unlimited'}
            - Categories to scrape: {categories_to_scrape or 'all categories'}
            - Store in database: {store_in_database}
            - Headless mode: {headless}
            
            Please execute the complete workflow and return detailed results.
            """,
            deps=deps
        )
        
        return result.data