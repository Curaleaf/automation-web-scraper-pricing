# Dispensary Web Scraping Automation Framework

A production-ready Pydantic AI agent-based automation framework for dispensary pricing data extraction with parallel processing, Snowflake integration, and GitHub Actions deployment.

## 🎯 Overview

This framework transforms the proven Jupyter notebook scraping logic into a scalable, automated system that:

- **Orchestrates parallel scraping** of 3 dispensary categories (Whole Flower, Pre-Rolls, Ground & Shake)
- **Preserves proven patterns** from the working notebook including regex patterns, rate limiting, and Florida store detection
- **Stores structured data** in Snowflake using standardized schema
- **Runs automatically** via GitHub Actions on schedule
- **Includes comprehensive testing** with both unit and integration tests

## 📊 Performance Metrics

Based on the original notebook performance:
- **2,544 rows** per category across **159 FL stores**
- **Parallel execution** of all 3 categories simultaneously
- **Rate-limited scraping** (700-1500ms delays) for respectful data collection
- **Automatic retry logic** for robust error handling

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- OpenAI API key (for the LLM agent)
- Snowflake account and credentials
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd automation-web-scraper-pricing/agents/dispensary_scraper
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   
   # Activate (Linux/Mac)
   source venv/bin/activate
   
   # Activate (Windows)
   venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials
   ```

### Basic Usage

```python
import asyncio
from agent import run_dispensary_scraping
from settings import load_settings

async def main():
    settings = load_settings()
    snowflake_config = settings.to_snowflake_config()
    
    # Run complete scraping workflow
    session = await run_dispensary_scraping(
        snowflake_config=snowflake_config,
        max_stores_per_category=5,  # Limit for testing
        store_in_database=True,
        headless=True
    )
    
    print(f"Scraped {session.total_products} products from {session.total_stores} stores")
    print(f"Session ID: {session.session_id}")
    print(f"Success: {session.success}")

if __name__ == "__main__":
    asyncio.run(main())
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file with the following configuration:

#### Required Variables

```bash
# LLM Configuration
LLM_API_KEY=your_openai_api_key_here

# Snowflake Configuration  
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
```

#### Optional Variables (with defaults)

```bash
# LLM Settings
LLM_PROVIDER=openai
LLM_MODEL=gpt-4
LLM_BASE_URL=https://api.openai.com/v1

# Snowflake Settings
SNOWFLAKE_ACCOUNT=CURALEAF-CURAPROD.snowflakecomputing.com
SNOWFLAKE_DATABASE=SANDBOX_EDW
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Scraping Configuration
MIN_DELAY_MS=700
MAX_DELAY_MS=1500
PAGE_TIMEOUT_MS=20000
MAX_RETRIES=3

# Parallel Processing
MAX_CONCURRENT_STORES=5
MAX_CONCURRENT_CATEGORIES=3

# Application Settings
APP_ENV=development
LOG_LEVEL=INFO
DEBUG=false
```

### Snowflake Table Mapping

The system uses these table mappings (configurable):

| Category | Snowflake Table |
|----------|----------------|
| Whole Flower | `TL_Scrape_WHOLE_FLOWER` |
| Pre-Rolls | `TL_Scrape_Pre_Rolls` |
| Ground & Shake | `TL_Scrape_Ground_Shake` |

## 🏗️ Architecture

### Core Components

```
agents/dispensary_scraper/
├── agent.py              # Main orchestration agent
├── tools.py              # Web scraping tool functions
├── models.py             # Pydantic data models
├── settings.py           # Configuration management
├── providers.py          # LLM model providers
├── dependencies.py       # Dependency injection
├── snowflake_client.py   # Database integration
├── requirements.txt      # Python dependencies
└── tests/                # Comprehensive test suite
```

### Data Flow

1. **Agent Orchestration**: Main agent coordinates parallel scraping
2. **Store Discovery**: Extract Florida dispensary locations
3. **Category Scraping**: Parallel processing of product categories
4. **Data Validation**: Pydantic models ensure data quality
5. **Database Storage**: Bulk insert into Snowflake tables
6. **Result Aggregation**: Session tracking and reporting

### Preserved Patterns

All critical patterns from the working notebook are preserved:

- **Florida Store Detection**: `looks_like_florida()` function with proven regex patterns
- **Price Extraction**: `PRICE_RE` pattern with fallback to product detail pages
- **Size Standardization**: `SIZE_MAP` for consistent weight conversions  
- **THC Extraction**: `THC_SINGLE_RE` and `THC_RANGE_RE` patterns
- **Deduplication**: Key-based filtering using `(store, slug, size, category)`
- **Rate Limiting**: Random delays (700-1500ms) between requests
- **Retry Logic**: Exponential backoff for failed operations

## 🧪 Testing

### Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run unit tests only
pytest tests/ -v -k "unit"

# Run with coverage
pytest tests/ -v --cov=. --cov-report=html

# Run specific test file
pytest tests/test_tools.py -v
```

### Test Categories

- **Unit Tests**: Test individual components with mocks
- **Integration Tests**: Test complete workflows with TestModel
- **Validation Tests**: Verify regex patterns and data processing
- **Mock Tests**: Test database operations without external dependencies

### Critical Test Cases

- ✅ Florida store detection accuracy
- ✅ Price extraction patterns  
- ✅ Size standardization and grams conversion
- ✅ Deduplication logic
- ✅ Snowflake integration
- ✅ Parallel execution
- ✅ Error handling and retry logic

## 🤖 GitHub Actions

### Automated Workflow

The system includes a complete CI/CD pipeline:

```yaml
# Scheduled runs daily at 6 AM EST
schedule:
  - cron: '0 11 * * *'

# Manual triggers with parameters
workflow_dispatch:
  inputs:
    max_stores_per_category: # Limit for testing
    categories_to_scrape:    # Specific categories
    store_in_database:       # Enable/disable DB storage
```

### Workflow Features

- **Environment Setup**: Python 3.11 with all dependencies
- **Playwright Installation**: Browser automation setup
- **Configuration Validation**: Pre-flight checks
- **Database Testing**: Snowflake connection verification
- **Parallel Execution**: All categories scraped simultaneously  
- **Result Reporting**: Comprehensive summary with metrics
- **Error Handling**: Graceful failure handling with notifications
- **Artifact Upload**: Logs preserved on failure

### Required GitHub Secrets

```bash
LLM_API_KEY                 # OpenAI API key
SNOWFLAKE_ACCOUNT          # Snowflake account identifier
SNOWFLAKE_USER             # Snowflake username
SNOWFLAKE_PASSWORD         # Snowflake password
SNOWFLAKE_DATABASE         # Database name
SNOWFLAKE_SCHEMA           # Schema name  
SNOWFLAKE_WAREHOUSE        # Warehouse name
SNOWFLAKE_ROLE            # Optional role
```

## 📊 Usage Examples

### Local Development

```python
# Run with limited stores for testing
session = await run_dispensary_scraping(
    snowflake_config=config,
    max_stores_per_category=10,
    store_in_database=False,  # Skip DB for testing
    headless=False            # Show browser for debugging
)
```

### Production Deployment

```python
# Run complete workflow
session = await run_dispensary_scraping(
    snowflake_config=config,
    categories_to_scrape=None,    # All categories
    store_in_database=True,       # Store results
    headless=True                 # Headless for production
)
```

### Custom Configuration

```python
from models import AgentConfig

# Create custom configuration
config = AgentConfig(
    min_delay_ms=1000,           # Slower rate limiting
    max_delay_ms=2000,
    max_concurrent_stores=3,     # Lower concurrency
    min_expected_products=50     # Validation threshold
)
```

### Agent Testing

```python
from pydantic_ai.models.test import TestModel
from agent import dispensary_agent

# Use TestModel for development
test_agent = dispensary_agent.override(model=TestModel())

result = await test_agent.run(
    "Scrape Whole Flower category",
    deps=mock_dependencies
)
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Configuration Errors

```bash
Error: "LLM_API_KEY cannot be empty"
```
**Solution**: Ensure `.env` file contains valid OpenAI API key
```bash
LLM_API_KEY=sk-your-actual-openai-key-here
```

#### 2. Snowflake Connection Issues

```bash
Error: "Failed to connect to Snowflake after 3 attempts"
```
**Solution**: Verify Snowflake credentials and network connectivity
```python
# Test connection manually
from snowflake_client import SnowflakeClient
client = SnowflakeClient(config)
await client.connect()
connection_ok = await client.test_connection()
print(f"Connection test: {connection_ok}")
```

#### 3. Playwright Issues

```bash
Error: "Browser not found"
```
**Solution**: Install Playwright browsers
```bash
playwright install chromium
playwright install-deps  # Linux dependencies
```

#### 4. Rate Limiting

```bash
Warning: "Request blocked by rate limiting"
```
**Solution**: Increase delays in configuration
```bash
MIN_DELAY_MS=1000
MAX_DELAY_MS=2500
```

#### 5. Memory Issues

```bash
Error: "Out of memory during large scraping"
```
**Solution**: Reduce concurrency limits
```bash
MAX_CONCURRENT_STORES=2
MAX_CONCURRENT_CATEGORIES=2
```

### Debug Mode

Enable detailed logging:

```bash
DEBUG=true
LOG_LEVEL=DEBUG
```

### Validation Commands

```bash
# Test configuration
python -c "from settings import load_settings; print('Config OK')"

# Test LLM connection  
python -c "from providers import validate_llm_configuration; print(validate_llm_configuration())"

# Test Snowflake connection
python -c "import asyncio; from snowflake_client import *; # ... test code"

# Run specific tests
pytest tests/test_tools.py::TestFloridaStoreDetection -v
```

## 📈 Monitoring & Metrics

### Key Metrics

- **Products Scraped**: Total products across all categories
- **Stores Processed**: Number of unique Florida stores
- **Success Rate**: Percentage of successful scraping operations
- **Duration**: Total execution time
- **Error Rate**: Failed operations per category

### Session Tracking

Each scraping session generates a unique ID and comprehensive metrics:

```python
session = ScrapingSession(
    session_id="unique-uuid",
    total_products=7632,      # Products across all categories
    total_stores=159,         # Unique FL stores processed
    success=True,             # Overall success status
    duration_seconds=1847.3,  # Total execution time
    results={                 # Per-category breakdown
        "Whole Flower": ScrapingResult(...),
        "Pre-Rolls": ScrapingResult(...),
        "Ground & Shake": ScrapingResult(...)
    }
)
```

## 🛡️ Security & Compliance

### Data Handling

- **PII**: No personally identifiable information is collected
- **Rate Limiting**: Respectful scraping with configurable delays
- **Error Handling**: Graceful failure without exposing sensitive data
- **Credentials**: Secure environment variable management

### Best Practices

- All API keys stored as environment variables
- Database credentials never logged or exposed  
- Retry logic prevents aggressive scraping
- User agent strings identify the scraper appropriately

## 🔄 Maintenance

### Regular Tasks

- **Monitor Success Rates**: Check GitHub Actions workflow results
- **Update Dependencies**: Regular security updates
- **Validate Patterns**: Ensure regex patterns remain effective
- **Review Performance**: Monitor scraping duration and product counts

### Updating Patterns

If website changes require pattern updates:

1. **Test Changes**: Use limited store testing first
2. **Validate Results**: Compare output to expected data structure  
3. **Update Tests**: Ensure test cases cover new patterns
4. **Deploy Gradually**: Test in staging before production

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create feature branch  
3. Install development dependencies
4. Run tests to ensure compatibility
5. Submit pull request with tests

### Code Standards

- **Type Hints**: All functions must have type annotations
- **Docstrings**: Google-style docstrings required
- **Testing**: New features must include tests
- **Formatting**: Use `black` for code formatting
- **Linting**: Code must pass `ruff` checks

## 📞 Support

### Getting Help

1. **Check Documentation**: Review this README and inline code comments
2. **Run Diagnostics**: Use validation commands to identify issues  
3. **Review Logs**: Check application logs for detailed error information
4. **Test Components**: Use unit tests to isolate problems

### Common Support Scenarios

- **Configuration Issues**: Environment variable problems
- **Performance Problems**: Rate limiting and concurrency tuning
- **Data Quality**: Validation and parsing issues  
- **Infrastructure**: Snowflake and GitHub Actions setup

---

## 📄 License

This project is part of the automation-web-scraper-pricing framework and follows the same licensing terms.

## 🙏 Acknowledgments

Built upon the proven scraping patterns from the original Jupyter notebook implementation, preserving all critical functionality while adding enterprise-grade automation capabilities.