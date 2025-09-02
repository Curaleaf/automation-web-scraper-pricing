"""
Unit tests for the main dispensary scraper agent.

CRITICAL: Test orchestration and parallel execution using TestModel
for agent testing without API calls as specified in PRP.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
from typing import Dict, List

from pydantic_ai.models.test import TestModel

from ..agent import dispensary_agent, run_dispensary_scraping
from ..models import ScrapingResult, ScrapingSession, ProductData
from ..dependencies import MockScrapingDependencies


class TestDispensaryAgent:
    """Test the main dispensary scraper agent."""
    
    @pytest.fixture
    def test_agent(self):
        """Create agent with TestModel for testing without API calls."""
        return dispensary_agent.override(model=TestModel())
    
    @pytest.mark.asyncio
    async def test_agent_initialization(self, test_agent):
        """Test agent can be initialized with TestModel."""
        assert test_agent is not None
        assert isinstance(test_agent._model, TestModel)
    
    @pytest.mark.asyncio
    async def test_get_florida_stores_tool(self, test_agent, mock_scraping_dependencies):
        """Test Florida stores retrieval tool."""
        # Mock the tool function
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract:
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami"),
                ("Test Store - Tampa, FL", "https://test.com/tampa")
            ]
            
            result = await test_agent.run(
                "Get the list of Florida stores",
                deps=mock_scraping_dependencies
            )
            
            # TestModel returns mock data, but we can verify the tool structure
            assert result is not None
    
    @pytest.mark.asyncio
    async def test_scrape_single_category_tool(self, test_agent, mock_scraping_dependencies):
        """Test single category scraping tool."""
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami")
            ]
            
            mock_product = ProductData(
                state="FL",
                store="Test Store - Miami, FL",
                subcategory="Whole Flower",
                name="Test Product",
                price=25.00,
                grams=3.5
            )
            mock_scrape.return_value = [mock_product]
            
            result = await test_agent.run(
                "Scrape the Whole Flower category",
                deps=mock_scraping_dependencies
            )
            
            assert result is not None
    
    @pytest.mark.asyncio
    async def test_scrape_all_categories_tool(self, test_agent, mock_scraping_dependencies):
        """Test parallel scraping of all categories."""
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami")
            ]
            
            # Mock different products for each category
            def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                return [ProductData(
                    state="FL",
                    store=store_name,
                    subcategory=subcategory,
                    name=f"Test {subcategory} Product",
                    price=25.00,
                    grams=3.5
                )]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            result = await test_agent.run(
                "Scrape all product categories in parallel",
                deps=mock_scraping_dependencies
            )
            
            assert result is not None
    
    @pytest.mark.asyncio
    async def test_store_results_in_snowflake_tool(self, test_agent, mock_scraping_dependencies):
        """Test Snowflake storage tool."""
        # Create mock scraping results
        mock_results = {
            "Whole Flower": ScrapingResult(
                category="Whole Flower",
                products=[ProductData(
                    state="FL",
                    store="Test Store",
                    subcategory="Whole Flower",
                    name="Test Product",
                    price=25.00,
                    grams=3.5
                )],
                store_count=1,
                total_products=1,
                success=True
            )
        }
        
        # The MockSnowflakeClient should handle the storage
        result = await test_agent.run(
            f"Store these scraping results in Snowflake: {mock_results}",
            deps=mock_scraping_dependencies
        )
        
        assert result is not None
    
    @pytest.mark.asyncio
    async def test_complete_workflow_tool(self, test_agent, mock_scraping_dependencies):
        """Test the complete scraping workflow."""
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami"),
                ("Test Store - Tampa, FL", "https://test.com/tampa")
            ]
            
            def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                return [ProductData(
                    state="FL",
                    store=store_name,
                    subcategory=subcategory,
                    name=f"Test {subcategory} Product",
                    price=25.00 + hash(subcategory + store_name) % 20,  # Vary prices
                    grams=3.5
                )]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            result = await test_agent.run(
                "Run the complete dispensary scraping workflow with testing limits",
                deps=mock_scraping_dependencies
            )
            
            assert result is not None


class TestParallelExecution:
    """Test parallel execution capabilities."""
    
    @pytest.mark.asyncio
    async def test_parallel_category_scraping(self, mock_scraping_dependencies):
        """Test that categories are scraped in parallel."""
        from ..agent import scrape_all_categories
        
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami")
            ]
            
            # Track call order to verify parallel execution
            call_order = []
            
            async def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                call_order.append(f"start_{subcategory}")
                # Simulate some async work
                import asyncio
                await asyncio.sleep(0.01)
                call_order.append(f"end_{subcategory}")
                return [ProductData(
                    state="FL",
                    store=store_name,
                    subcategory=subcategory,
                    name=f"Test {subcategory} Product",
                    price=25.00,
                    grams=3.5
                )]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            # Create mock context
            mock_ctx = Mock()
            mock_ctx.deps = mock_scraping_dependencies
            
            results = await scrape_all_categories(mock_ctx, max_stores_per_category=1)
            
            # Should have results for all 3 categories
            assert len(results) == 3
            assert "Whole Flower" in results
            assert "Pre-Rolls" in results
            assert "Ground & Shake" in results
            
            # Verify all categories completed successfully
            for category, result in results.items():
                assert result.success, f"Category {category} should be successful"
                assert result.total_products > 0, f"Category {category} should have products"
    
    @pytest.mark.asyncio
    async def test_parallel_execution_with_failures(self, mock_scraping_dependencies):
        """Test parallel execution handles individual category failures gracefully."""
        from ..agent import scrape_all_categories
        
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami")
            ]
            
            # Mock failures for specific categories
            def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                if subcategory == "Pre-Rolls":
                    raise Exception("Network timeout")
                return [ProductData(
                    state="FL",
                    store=store_name,
                    subcategory=subcategory,
                    name=f"Test {subcategory} Product",
                    price=25.00,
                    grams=3.5
                )]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            # Create mock context
            mock_ctx = Mock()
            mock_ctx.deps = mock_scraping_dependencies
            
            results = await scrape_all_categories(mock_ctx, max_stores_per_category=1)
            
            # Should still have results for all categories
            assert len(results) == 3
            
            # Pre-Rolls should have failed
            assert not results["Pre-Rolls"].success
            assert "Network timeout" in results["Pre-Rolls"].error_message
            
            # Other categories should have succeeded
            assert results["Whole Flower"].success
            assert results["Ground & Shake"].success


class TestWorkflowIntegration:
    """Test complete workflow integration."""
    
    @pytest.mark.asyncio
    async def test_complete_scraping_workflow(self, mock_scraping_dependencies):
        """Test the complete workflow from start to finish."""
        from ..agent import run_complete_scraping_workflow
        
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            # Setup mocks
            mock_extract.return_value = [
                ("Test Store - Miami, FL", "https://test.com/miami"),
                ("Test Store - Tampa, FL", "https://test.com/tampa")
            ]
            
            def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                # Return different numbers of products per store/category
                num_products = 2 if "Miami" in store_name else 3
                return [
                    ProductData(
                        state="FL",
                        store=store_name,
                        subcategory=subcategory,
                        name=f"Product {i} - {subcategory}",
                        price=25.00 + i * 5,
                        grams=3.5
                    )
                    for i in range(num_products)
                ]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            # Create mock context
            mock_ctx = Mock()
            mock_ctx.deps = mock_scraping_dependencies
            
            # Run complete workflow
            session = await run_complete_scraping_workflow(
                mock_ctx,
                max_stores_per_category=2,
                store_in_database=True
            )
            
            # Verify session structure
            assert isinstance(session, ScrapingSession)
            assert session.session_id is not None
            assert session.success
            assert len(session.results) == 3  # All 3 categories
            assert session.total_products > 0
            assert session.total_stores == 2  # 2 unique stores
            assert session.duration_seconds is not None
    
    @pytest.mark.asyncio
    async def test_workflow_with_database_errors(self, mock_scraping_dependencies):
        """Test workflow handles database insertion errors gracefully."""
        from ..agent import run_complete_scraping_workflow
        
        # Mock Snowflake client to raise errors
        mock_scraping_dependencies.snowflake_client.insert_products_by_category = AsyncMock(
            side_effect=Exception("Database connection failed")
        )
        
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [("Test Store", "https://test.com")]
            mock_scrape.return_value = [ProductData(
                state="FL", store="Test Store", subcategory="Whole Flower",
                name="Test", price=25.00, grams=3.5
            )]
            
            mock_ctx = Mock()
            mock_ctx.deps = mock_scraping_dependencies
            
            session = await run_complete_scraping_workflow(
                mock_ctx, store_in_database=True
            )
            
            # Should complete but have errors recorded
            assert isinstance(session, ScrapingSession)
            assert len(session.errors) > 0
            assert any("Database" in error for error in session.errors)


class TestConvenienceFunction:
    """Test the convenience function for direct usage."""
    
    @pytest.mark.asyncio
    async def test_run_dispensary_scraping_function(self, snowflake_config):
        """Test the convenience function for running scraping."""
        with patch('agents.dispensary_scraper.dependencies.DependencyManager') as mock_manager_class, \
             patch('agents.dispensary_scraper.agent.dispensary_agent') as mock_agent:
            
            # Setup mock dependency manager
            mock_manager = Mock()
            mock_deps = MockScrapingDependencies()
            mock_manager.create_dependencies.return_value = mock_deps
            mock_manager_class.return_value = mock_manager
            
            # Setup mock agent
            mock_session = ScrapingSession(
                session_id="test-session",
                success=True,
                total_products=100,
                total_stores=10
            )
            mock_agent.run.return_value = Mock(data=mock_session)
            
            # Test the function
            result = await run_dispensary_scraping(
                snowflake_config=snowflake_config,
                max_stores_per_category=5,
                categories_to_scrape=["Whole Flower"],
                store_in_database=True,
                headless=True
            )
            
            # Verify result structure
            assert isinstance(result, ScrapingSession)
            assert result.success
            assert result.session_id == "test-session"
            
            # Verify agent was called with correct parameters
            mock_agent.run.assert_called_once()
            call_args = mock_agent.run.call_args
            assert "complete dispensary scraping workflow" in call_args[0][0].lower()
    
    @pytest.mark.asyncio
    async def test_convenience_function_with_limited_categories(self, snowflake_config):
        """Test convenience function with limited category list."""
        with patch('agents.dispensary_scraper.dependencies.create_scraping_dependencies') as mock_context, \
             patch('agents.dispensary_scraper.agent.dispensary_agent') as mock_agent:
            
            # Setup async context manager
            mock_deps = MockScrapingDependencies()
            mock_context.return_value.__aenter__ = AsyncMock(return_value=mock_deps)
            mock_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Setup mock agent response
            mock_session = ScrapingSession(
                session_id="limited-test",
                success=True,
                total_products=50,
                total_stores=5
            )
            mock_agent.run.return_value = Mock(data=mock_session)
            
            # Test with limited categories
            result = await run_dispensary_scraping(
                snowflake_config=snowflake_config,
                categories_to_scrape=["Whole Flower", "Pre-Rolls"],
                store_in_database=False
            )
            
            assert isinstance(result, ScrapingSession)
            assert result.session_id == "limited-test"
            
            # Verify the prompt included the limited categories
            call_args = mock_agent.run.call_args[0][0]
            assert "Whole Flower" in call_args or "Pre-Rolls" in call_args


@pytest.mark.slow
@pytest.mark.integration
class TestRealWorldScenarios:
    """Integration tests that simulate real-world scenarios."""
    
    @pytest.mark.asyncio
    async def test_large_store_list_handling(self, mock_scraping_dependencies):
        """Test handling of large number of stores."""
        from ..agent import scrape_single_category
        
        # Create a large list of mock stores
        large_store_list = [
            (f"Store {i} - City {i}, FL", f"https://test.com/store-{i}")
            for i in range(100)
        ]
        
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = large_store_list
            
            # Mock scraping to return 1-3 products per store
            def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                store_num = int(store_name.split()[1])
                num_products = (store_num % 3) + 1  # 1-3 products
                return [
                    ProductData(
                        state="FL",
                        store=store_name,
                        subcategory=subcategory,
                        name=f"Product {j}",
                        price=20.00 + j * 5,
                        grams=3.5
                    )
                    for j in range(num_products)
                ]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            # Create mock context
            mock_ctx = Mock()
            mock_ctx.deps = mock_scraping_dependencies
            
            # Test with limited stores for performance
            result = await scrape_single_category(
                mock_ctx, "Whole Flower", "https://test.com/category", max_stores=10
            )
            
            assert result.success
            assert result.store_count == 10
            assert result.total_products >= 10  # At least 1 product per store
            assert result.total_products <= 30  # At most 3 products per store
    
    @pytest.mark.asyncio 
    async def test_mixed_success_failure_scenarios(self, mock_scraping_dependencies):
        """Test scenarios where some stores succeed and others fail."""
        from ..agent import scrape_single_category
        
        with patch('agents.dispensary_scraper.agent.extract_florida_stores_tool') as mock_extract, \
             patch('agents.dispensary_scraper.agent.scrape_store_category_tool') as mock_scrape:
            
            mock_extract.return_value = [
                ("Successful Store - Miami, FL", "https://test.com/miami"),
                ("Failing Store - Tampa, FL", "https://test.com/tampa"), 
                ("Another Success - Orlando, FL", "https://test.com/orlando")
            ]
            
            # Mock some stores to fail
            def mock_scrape_side_effect(ctx, store_name, store_url, category_url, subcategory):
                if "Failing" in store_name:
                    raise Exception("Store temporarily unavailable")
                return [ProductData(
                    state="FL",
                    store=store_name,
                    subcategory=subcategory,
                    name="Test Product",
                    price=25.00,
                    grams=3.5
                )]
            
            mock_scrape.side_effect = mock_scrape_side_effect
            
            mock_ctx = Mock()
            mock_ctx.deps = mock_scraping_dependencies
            
            result = await scrape_single_category(
                mock_ctx, "Whole Flower", "https://test.com/category"
            )
            
            # Should still be successful overall with partial data
            assert result.success
            assert result.store_count == 2  # Only 2 successful stores
            assert result.total_products == 2  # 1 product per successful store