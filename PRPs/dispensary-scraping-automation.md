name: "Dispensary Web Scraping Automation Framework"
description: |

## Purpose
Complete Pydantic AI agent-based automation framework for dispensary pricing data extraction with parallel processing, Snowflake integration, and GitHub Actions deployment.

## Core Principles
1. **Context is King**: Leverage existing notebook patterns and proven web scraping techniques
2. **Validation Loops**: Comprehensive testing with Playwright and database validation
3. **Information Dense**: Use established regex patterns, rate limiting, and error handling
4. **Progressive Success**: Start with core scraping, validate, then add orchestration
5. **Global rules**: Follow all rules in CLAUDE.md and examples/CLAUDE.md

---

## Goal
Transform the existing Jupyter notebook web scraping logic into a production-ready Pydantic AI agent system that orchestrates parallel scraping of multiple dispensary categories and automatically stores results in Snowflake database.

## Why
- **Scalability**: Current notebook approach doesn't scale across multiple categories in parallel
- **Automation**: Manual execution needs to be replaced with GitHub Actions workflow
- **Data Management**: Structured data flow into Snowflake for analytics and reporting  
- **Maintainability**: Agent-based architecture allows for easier testing and modification
- **Reliability**: Proper error handling, retry mechanisms, and data validation

## What
A complete automation framework that:
- Orchestrates parallel scraping of 3 dispensary categories (Whole Flower, Pre-Rolls, Ground & Shake)
- Uses proven Playwright-based extraction patterns from existing notebook
- Stores structured data in Snowflake using standardized schema
- Runs via GitHub Actions on schedule
- Includes comprehensive testing and monitoring

### Success Criteria
- [ ] All 3 categories scrape in parallel and complete successfully
- [ ] Data matches existing notebook output structure and quality
- [ ] Successful Snowflake database integration with proper table management
- [ ] GitHub Actions workflow executes end-to-end without intervention
- [ ] Comprehensive test coverage with both unit and integration tests
- [ ] Performance equals or exceeds current notebook (2,544 rows across 159 FL stores per category)

## All Needed Context

### Documentation & References
```yaml
# MUST READ - Include these in your context window
- url: https://ai.pydantic.dev/agents/
  why: Core Pydantic AI agent patterns and architecture
  
- url: https://ai.pydantic.dev/tools/
  why: Tool integration patterns for web scraping functions
  
- url: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector
  why: Snowflake Python connector for database operations
  
- file: examples/Web scrape attempt.ipynb
  why: Complete working scraping implementation with all patterns to preserve
  critical: Contains proven regex patterns, rate limiting, Florida store detection, price extraction
  
- file: examples/CLAUDE.md
  why: Web scraping best practices and data structure standards
  critical: Async patterns, error handling, deduplication keys, size standardization
  
- file: use-cases/agent-factory-with-subagents/examples/main_agent_reference/
  why: Pydantic AI agent structure patterns (agent.py, tools.py, settings.py, providers.py)
  
- file: use-cases/agent-factory-with-subagents/examples/main_agent_reference/settings.py
  why: Environment variable management with python-dotenv
  critical: API key management, configuration validation
  
- docfile: CLAUDE.md
  why: Global project rules for modularity, testing, and code structure
```

### Current Codebase tree
```bash
automation-web-scraper-pricing/
├── CLAUDE.md                    # Global project rules
├── INITIAL.md                   # Feature requirements 
├── examples/
│   ├── CLAUDE.md                # Web scraping architecture guide
│   └── Web scrape attempt.ipynb # Working scraping implementation
├── use-cases/agent-factory-with-subagents/examples/main_agent_reference/
│   ├── agent.py                 # Pydantic AI agent pattern
│   ├── tools.py                 # Tool function examples
│   ├── settings.py              # Environment config pattern
│   └── providers.py             # Model provider setup
```

### Desired Codebase tree with files to be added and responsibility of file
```bash
automation-web-scraper-pricing/
├── agents/
│   └── dispensary_scraper/
│       ├── agent.py             # Main orchestration agent
│       ├── tools.py             # Web scraping tool functions  
│       ├── models.py            # Pydantic data models
│       ├── settings.py          # Configuration management
│       ├── providers.py         # LLM model providers
│       ├── dependencies.py      # External service dependencies
│       ├── snowflake_client.py  # Snowflake integration
│       ├── requirements.txt     # Python dependencies
│       ├── .env.example         # Environment template
│       ├── README.md            # Usage documentation
│       └── tests/               # Test suite
│           ├── test_agent.py    # Agent orchestration tests
│           ├── test_tools.py    # Scraping function tests
│           ├── test_snowflake.py # Database integration tests
│           └── conftest.py      # Test configuration
├── .github/
│   └── workflows/
│       └── scrape-dispensary.yml # GitHub Actions workflow
```

### Known Gotchas of our codebase & Library Quirks
```python
# CRITICAL: Playwright requires specific async setup on Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# CRITICAL: Jupyter notebook environments need nest_asyncio
import nest_asyncio; nest_asyncio.apply()

# CRITICAL: Rate limiting is essential - random delays 700-1500ms between requests
await page.wait_for_timeout(random.randint(700,1500))

# CRITICAL: Florida store detection regex patterns are proven and should be preserved
def looks_like_florida(href, text):
    t=(text or "").upper(); h=(href or "").lower()
    return (", FL" in t) or t.endswith(" FL") or " FL " in t

# CRITICAL: Deduplication key prevents duplicates across store visits
key=(store_name, slug, size, SUBCATEGORY)

# CRITICAL: Snowflake requires proper connection management and error handling
# Connection can timeout, need retry logic

# CRITICAL: Pydantic AI agents need deps_type for external dependencies
@dataclass
class ScrapingDependencies:
    playwright_context: Any
    snowflake_client: Any

# CRITICAL: Use python-dotenv exactly as shown in main_agent_reference/settings.py
from dotenv import load_dotenv
load_dotenv()  # Must be called before Settings class instantiation

# CRITICAL: Size standardization mapping is essential for price_per_g calculations
SIZE_MAP = {"0.5g":0.5,"1g":1.0,"2g":2.0,"3.5g":3.5,"7g":7.0,"10g":10.0,"14g":14.0,"28g":28.0}
```

## Implementation Blueprint

### Data models and structure
Create the core data models to ensure type safety and consistency.
```python
# Pydantic models matching existing notebook output structure
class ProductData(BaseModel):
    state: str = "FL"
    store: str
    subcategory: str  # "Whole Flower", "Pre-Rolls", "Ground & Shake"
    name: str
    brand: Optional[str] = None
    strain_type: Optional[str] = None  # "Indica", "Sativa", "Hybrid"
    thc_pct: Optional[float] = None
    size_raw: Optional[str] = None
    grams: Optional[float] = None
    price: Optional[float] = None
    price_per_g: Optional[float] = None
    url: Optional[str] = None
    scraped_at: datetime = Field(default_factory=datetime.now)

class ScrapingResult(BaseModel):
    category: str
    products: List[ProductData]
    store_count: int
    total_products: int
    success: bool
    error_message: Optional[str] = None

class SnowflakeConfig(BaseModel):
    account: str = "CURALEAF-CURAPROD.snowflakecomputing.com"
    database: str = "SANDBOX_EDW" 
    schema: str = "ANALYTICS"
    tables: Dict[str, str] = {
        "Whole Flower": "TL_Scrape_WHOLE_FLOWER",
        "Pre-Rolls": "TL_Scrape_Pre_Rolls",
        "Ground & Shake": "TL_Scrape_Ground_Shake"
    }
```

### List of tasks to be completed to fulfill the PRP in the order they should be completed

```yaml
Task 1 - Setup Agent Directory Structure:
CREATE agents/dispensary_scraper/:
  - Create base directory structure
  - Copy main_agent_reference patterns as starting point
  - Create __init__.py for package initialization
  
Task 2 - Extract and Modularize Scraping Logic:
EXTRACT from examples/Web scrape attempt.ipynb:
  - PRESERVE all regex patterns (PRICE_RE, SIZE_RE, THC_SINGLE_RE, THC_RANGE_RE)  
  - PRESERVE looks_like_florida() function exactly
  - PRESERVE extract_fl_store_links() async function
  - PRESERVE load_all() pagination logic
  - PRESERVE extract_price_from_card() and extract_price_from_pdp() functions
  - PRESERVE extract_brand_from_card() and extract_brand_from_pdp() functions
  - PRESERVE scrape_category() main logic
CREATE agents/dispensary_scraper/tools.py:
  - Convert notebook functions to Pydantic AI tools with @agent.tool decorators
  - Maintain async patterns and error handling
  - Add proper type hints and docstrings

Task 3 - Create Pydantic Models:
CREATE agents/dispensary_scraper/models.py:
  - Mirror existing notebook data structure exactly
  - Add validation for required fields
  - Include scraped_at timestamp for tracking

Task 4 - Setup Configuration Management:
CREATE agents/dispensary_scraper/settings.py:
  - MIRROR pattern from: main_agent_reference/settings.py
  - ADD Snowflake connection parameters
  - ADD Playwright configuration options
  - PRESERVE python-dotenv usage pattern

CREATE agents/dispensary_scraper/.env.example:
  - Include all required environment variables
  - Document Snowflake connection requirements
  - Include LLM API key requirements

Task 5 - Create Snowflake Integration:
CREATE agents/dispensary_scraper/snowflake_client.py:
  - Use snowflake-connector-python for database operations
  - Implement connection pooling and retry logic
  - Create methods for table insertion with proper error handling
  - Support batch insertion for performance

Task 6 - Create Dependencies Management:
CREATE agents/dispensary_scraper/dependencies.py:
  - Define ScrapingDependencies dataclass for Playwright context and Snowflake client
  - Setup dependency injection for agent tools
  - Include connection lifecycle management

Task 7 - Create Main Agent:
CREATE agents/dispensary_scraper/agent.py:
  - MIRROR pattern from: main_agent_reference/research_agent.py
  - Create orchestration agent that runs 3 categories in parallel
  - Register all scraping tools from tools.py
  - Implement proper error handling and result aggregation

Task 8 - Create Provider Configuration:
CREATE agents/dispensary_scraper/providers.py:
  - MIRROR pattern from: main_agent_reference/providers.py  
  - Setup model provider configuration
  - Support multiple LLM providers for flexibility

Task 9 - Create Requirements File:
CREATE agents/dispensary_scraper/requirements.txt:
  - Include pydantic-ai, playwright, snowflake-connector-python
  - Include pandas for data manipulation compatibility
  - Include python-dotenv for environment management
  - Pin versions for stability

Task 10 - Create Comprehensive Test Suite:
CREATE agents/dispensary_scraper/tests/:
  - test_agent.py: Test orchestration and parallel execution
  - test_tools.py: Test all scraping functions with mocked Playwright
  - test_snowflake.py: Test database operations with test database
  - conftest.py: Setup test fixtures and mocked dependencies
  - Use TestModel for agent testing without API calls

Task 11 - Create GitHub Actions Workflow:
CREATE .github/workflows/scrape-dispensary.yml:
  - Setup Python environment with required dependencies
  - Install and configure Playwright browsers  
  - Run scraping agent with proper secret management
  - Handle workflow scheduling and error notifications

Task 12 - Create Documentation:
CREATE agents/dispensary_scraper/README.md:
  - Document installation and setup procedures
  - Provide usage examples for local development
  - Document environment variables and configuration
  - Include troubleshooting guide for common issues
```

### Per task pseudocode as needed added to each task

```python
# Task 2 - Tools Implementation
# Pseudocode for preserving notebook patterns in Pydantic AI tools

@agent.tool
async def scrape_category_tool(
    ctx: RunContext[ScrapingDependencies], 
    category_url: str,
    subcategory: str,
    store_name: str
) -> List[ProductData]:
    # PATTERN: Use existing browser context from dependencies
    page = ctx.deps.playwright_context.new_page()
    
    # PRESERVE: Exact scraping logic from notebook
    await page.goto(category_url, wait_until="domcontentloaded")
    await load_all(page)  # Preserve pagination logic
    
    # PRESERVE: Product extraction patterns
    name_links = await page.locator("a[href*='/product/']:not(:has(img))").all()
    
    products = []
    seen = set()  # CRITICAL: Deduplication key preservation
    
    for link in name_links:
        # PRESERVE: All extraction patterns from notebook
        # - price extraction with fallback to PDP
        # - brand extraction with fallback
        # - strain type and THC percentage parsing
        # - size standardization and grams conversion
        pass
    
    return [ProductData(**product_dict) for product_dict in products]

# Task 5 - Snowflake Integration
# Pseudocode for database operations with proper error handling

class SnowflakeClient:
    async def insert_products(self, products: List[ProductData], table_name: str):
        # PATTERN: Connection management with retry logic
        for attempt in range(3):
            try:
                with self.get_connection() as conn:
                    # CRITICAL: Batch insert for performance
                    df = pd.DataFrame([p.dict() for p in products])
                    # Use write_pandas for efficient bulk insert
                    success, nchunks, nrows, _ = write_pandas(
                        conn, df, table_name,
                        database=self.config.database,
                        schema=self.config.schema
                    )
                    return success
            except Exception as e:
                if attempt == 2: raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

# Task 7 - Agent Implementation  
# Pseudocode for parallel orchestration

async def run_parallel_scraping(self) -> Dict[str, ScrapingResult]:
    # PATTERN: Concurrent execution of multiple categories
    categories = [
        ("https://www.trulieve.com/category/flower/whole-flower", "Whole Flower"),
        ("https://www.trulieve.com/category/flower/pre-rolls", "Pre-Rolls"),
        ("https://www.trulieve.com/category/flower/ground-shake", "Ground & Shake")
    ]
    
    tasks = []
    for category_url, subcategory in categories:
        task = self.scrape_and_store_category(category_url, subcategory)
        tasks.append(task)
    
    # CRITICAL: Parallel execution while maintaining rate limiting per category
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return self.aggregate_results(results)
```

### Integration Points
```yaml
PLAYWRIGHT:
  - browser: "chromium with --no-sandbox args for server environments"
  - context: "Custom user agent matching existing notebook"
  - timeout: "20 second timeouts with proper exception handling"

SNOWFLAKE:
  - connection: "Use environment variables for credentials"
  - tables: "Maintain existing table schema and names exactly"
  - insert: "Use write_pandas for efficient bulk operations"
  
GITHUB_ACTIONS:
  - secrets: "Store Snowflake credentials and API keys securely"
  - schedule: "Add cron trigger for automated execution"  
  - notifications: "Email on failure, summary on success"

ENVIRONMENT:
  - python: "3.11+ for compatibility with all dependencies"
  - virtual_env: "Use venv_linux as specified in CLAUDE.md"
  - async: "Proper Windows event loop policy for cross-platform"
```

## Validation Loop

### Level 1: Syntax & Style
```bash
# Run these FIRST - fix any errors before proceeding
cd agents/dispensary_scraper/
ruff check . --fix                 # Auto-fix what's possible  
mypy .                            # Type checking

# Expected: No errors. If errors, READ the error and fix.
```

### Level 2: Unit Tests 
```bash
# CREATE comprehensive test suite with these critical test cases:

# Test scraping functions with mocked Playwright
def test_extract_fl_store_links_parsing():
    """Verify Florida store detection logic"""
    # Mock page with known store data
    # Verify looks_like_florida() function accuracy

def test_price_extraction_patterns():
    """Verify regex patterns extract prices correctly""" 
    # Test PRICE_RE against various price formats
    # Test edge cases like multiple prices, no prices

def test_size_standardization():
    """Verify SIZE_MAP and grams conversion"""
    # Test all supported size formats
    # Verify price_per_g calculations

def test_deduplication_logic():
    """Verify duplicate prevention works"""
    # Test deduplication key generation
    # Verify duplicates are properly filtered

def test_snowflake_integration():
    """Test database operations"""
    # Use test database for insertion tests
    # Verify connection handling and retry logic

# Run and iterate until passing:
cd agents/dispensary_scraper/
uv run pytest tests/ -v --cov=. --cov-report=html
# If failing: Read error, understand root cause, fix code, re-run
```

### Level 3: Integration Test
```bash
# Test full scraping workflow with TestModel
cd agents/dispensary_scraper/

# Test agent orchestration
python -c "
from agent import dispensary_agent
from pydantic_ai.models.test import TestModel

# Override with TestModel for testing
test_agent = dispensary_agent.override(model=TestModel())
result = await test_agent.run('Scrape all categories')
print(f'Test result: {result}')
"

# Test with actual Playwright (single store for speed)
python -c "
from agent import run_scraping_workflow
result = await run_scraping_workflow(test_mode=True)  # Single store only
assert result['success'] == True
assert len(result['categories']) == 3
print('Integration test passed')
"
```

## Final validation Checklist
- [ ] All tests pass: `uv run pytest agents/dispensary_scraper/tests/ -v`
- [ ] No linting errors: `uv run ruff check agents/dispensary_scraper/`
- [ ] No type errors: `uv run mypy agents/dispensary_scraper/`
- [ ] Scraping produces expected data structure and volumes
- [ ] Snowflake integration successfully inserts data
- [ ] GitHub Actions workflow completes end-to-end
- [ ] All regex patterns from notebook preserved and working
- [ ] Rate limiting and error handling robust under failure conditions
- [ ] Documentation complete with setup and usage instructions

---

## Anti-Patterns to Avoid
- ❌ Don't modify proven regex patterns from the notebook - they work
- ❌ Don't skip rate limiting - sites will block aggressive scraping
- ❌ Don't hardcode Snowflake credentials - use environment variables
- ❌ Don't ignore Florida store detection logic - it's complex but necessary
- ❌ Don't change data structure - maintain compatibility with existing analytics
- ❌ Don't skip deduplication - it prevents data quality issues
- ❌ Don't use sync functions in async Playwright context
- ❌ Don't forget to handle Playwright browser lifecycle properly

---

## Quality Confidence Assessment

**Confidence Level: 9/10**

**Strengths:**
- Complete working implementation exists in notebook to reference
- Proven scraping patterns with known performance metrics
- Comprehensive Pydantic AI examples to follow
- Clear data model and Snowflake integration requirements
- Extensive documentation and error handling patterns identified

**Risk Mitigation:**
- All critical regex patterns and logic preserved from working notebook
- Comprehensive test suite covers edge cases and integration points
- Incremental validation at each stage prevents compound errors
- Fallback strategies documented for common failure modes

**Expected Success Rate:** 95% one-pass implementation success with minor adjustments for environment-specific configuration.