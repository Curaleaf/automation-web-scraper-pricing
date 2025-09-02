"""
Flexible provider configuration for LLM models.

CRITICAL: Follows the exact pattern from main_agent_reference/providers.py
with support for multiple LLM providers for flexibility.
"""

from typing import Optional, Union
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.models.test import TestModel
from .settings import settings


def get_llm_model(model_choice: Optional[str] = None) -> Union[OpenAIModel, TestModel]:
    """
    Get LLM model configuration based on environment variables.
    
    CRITICAL: Mirrors main_agent_reference/providers.py pattern exactly.
    
    Args:
        model_choice: Optional override for model choice
    
    Returns:
        Configured LLM model (OpenAI-compatible or TestModel)
    """
    llm_choice = model_choice or settings.llm_model
    base_url = settings.llm_base_url
    api_key = settings.llm_api_key
    
    # Use TestModel for testing/development if specified
    if llm_choice.lower() in ("test", "testmodel", "mock"):
        return TestModel()
    
    # Create provider based on configuration
    provider = OpenAIProvider(base_url=base_url, api_key=api_key)
    
    return OpenAIModel(llm_choice, provider=provider)


def get_test_model() -> TestModel:
    """
    Get TestModel for testing and development.
    
    Returns:
        TestModel instance for testing without API calls
    """
    return TestModel()


def get_model_info() -> dict:
    """
    Get information about current model configuration.
    
    Returns:
        Dictionary with model configuration info
    """
    return {
        "llm_provider": settings.llm_provider,
        "llm_model": settings.llm_model,
        "llm_base_url": settings.llm_base_url,
        "app_env": settings.app_env,
        "debug": settings.debug,
        "snowflake_account": settings.snowflake_account,
        "snowflake_database": settings.snowflake_database,
        "min_delay_ms": settings.min_delay_ms,
        "max_delay_ms": settings.max_delay_ms,
    }


def validate_llm_configuration() -> bool:
    """
    Validate that LLM configuration is properly set.
    
    Returns:
        True if configuration is valid
    """
    try:
        # Check if we can create a model instance
        model = get_llm_model()
        return True
    except Exception as e:
        print(f"LLM configuration validation failed: {e}")
        return False


def validate_scraping_configuration() -> bool:
    """
    Validate that scraping configuration is properly set.
    
    Returns:
        True if scraping configuration is valid
    """
    try:
        # Validate required settings
        required_fields = [
            settings.snowflake_user,
            settings.snowflake_password,
            settings.snowflake_account,
            settings.llm_api_key
        ]
        
        for field in required_fields:
            if not field or not field.strip():
                return False
        
        # Validate timing configuration
        if settings.min_delay_ms <= 0 or settings.max_delay_ms <= 0:
            return False
        
        if settings.min_delay_ms > settings.max_delay_ms:
            return False
        
        # Validate retry configuration
        if settings.max_retries < 0 or settings.max_retries > 10:
            return False
        
        # Validate concurrency limits
        if settings.max_concurrent_stores < 1 or settings.max_concurrent_categories < 1:
            return False
        
        return True
        
    except Exception as e:
        print(f"Scraping configuration validation failed: {e}")
        return False


def get_provider_for_testing() -> TestModel:
    """
    Get a test provider for unit testing.
    
    This is useful for testing agent behavior without making actual LLM API calls.
    
    Returns:
        TestModel: Test model provider
    """
    return TestModel()


def create_model_with_override(
    model_name: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None
) -> Union[OpenAIModel, TestModel]:
    """
    Create a model with configuration overrides.
    
    Useful for testing different configurations or using different
    API endpoints without changing the global settings.
    
    Args:
        model_name: Override model name
        api_key: Override API key
        base_url: Override base URL
        
    Returns:
        Configured model
    """
    model_choice = model_name or settings.llm_model
    
    # Use TestModel for testing if specified
    if model_choice.lower() in ("test", "testmodel", "mock"):
        return TestModel()
    
    # Use provided overrides or fall back to settings
    final_api_key = api_key or settings.llm_api_key
    final_base_url = base_url or settings.llm_base_url
    
    provider = OpenAIProvider(base_url=final_base_url, api_key=final_api_key)
    return OpenAIModel(model_choice, provider=provider)


def supports_provider(provider_name: str) -> bool:
    """
    Check if a provider is supported.
    
    Args:
        provider_name: Name of the provider to check
        
    Returns:
        True if provider is supported
    """
    supported_providers = ["openai", "test", "testmodel", "mock"]
    return provider_name.lower() in supported_providers


def get_available_models() -> list:
    """
    Get list of available model configurations.
    
    Returns:
        List of available model names/types
    """
    return [
        "gpt-4",
        "gpt-4-turbo", 
        "gpt-3.5-turbo",
        "test",  # For testing
    ]


def get_recommended_model() -> str:
    """
    Get recommended model for dispensary scraping tasks.
    
    Returns:
        Recommended model name
    """
    # GPT-4 is recommended for complex web scraping and orchestration tasks
    return "gpt-4"