# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Context Engineering template repository with examples for web scraping automation, specifically focused on pricing data extraction from dispensary websites using Playwright and Python.

## Architecture & Structure

### Core Pattern: Web Scraping Automation
- **Async/await pattern**: All scraping code uses Python asyncio for concurrent operations
- **Playwright-based**: Uses async Playwright for web automation and data extraction
- **Data export**: Outputs to both CSV and Parquet formats using pandas
- **Modular extraction**: Separate functions for price, brand, store location, and product data extraction

### Code Organization
- **Single notebook pattern**: Complex scraping logic in Jupyter notebooks (`.ipynb`)
- **Reusable functions**: Common extraction patterns (price parsing, brand detection, FL store filtering)
- **Category-based scraping**: Separate scraping flows for different product categories (whole flower, pre-rolls, ground & shake)

## Key Technical Patterns

### Web Scraping Best Practices
- **Rate limiting**: Random delays between requests (700-1500ms)
- **Timeout handling**: 20-second timeouts with proper exception handling
- **Pagination**: "Load More" button clicking with infinite scroll detection
- **Duplicate prevention**: Key-based deduplication using (store, product_slug, size, category)
- **Fallback strategies**: Card extraction + PDP (Product Detail Page) fallback for missing data

### Data Extraction Patterns
```python
# Price extraction using regex patterns
PRICE_RE = re.compile(r"\$\s*([0-9]+(?:\.[0-9]{2})?)")

# Size standardization with mapping
SIZE_MAP = {"0.5g":0.5,"1g":1.0,"2g":2.0,"3.5g":3.5,"7g":7.0,"10g":10.0,"14g":14.0,"28g":28.0}

# Florida store detection
def looks_like_florida(href, text):
    return (", FL" in text.upper()) or text.upper().endswith(" FL")
```

### Error Handling
- **Try-catch wrapping**: All extraction operations wrapped in try-except blocks
- **Graceful degradation**: Continue processing even if individual items fail
- **Context manager usage**: Proper browser/page cleanup with async context managers

## Development Guidelines

### Environment Setup
- **Windows compatibility**: Uses `asyncio.WindowsSelectorEventLoopPolicy()` for Windows
- **Nested asyncio**: Handles Jupyter notebook event loops with `nest_asyncio`
- **Playwright dependencies**: Requires `uv pip install playwright` and `playwright install`

### Data Structure Standards
```python
# Standard output schema for all scrapers
{
    "state": "FL",
    "store": store_name,
    "subcategory": category,
    "name": product_name,
    "brand": brand_name,
    "strain_type": "Indica|Sativa|Hybrid",
    "thc_pct": float,
    "size_raw": "3.5g",
    "grams": 3.5,
    "price": float,
    "price_per_g": float,
    "url": product_url
}
```

### Context Engineering Components

#### PRP System (Product Requirements Prompts)
- Use `/generate-prp` command to create comprehensive implementation blueprints
- PRPs include complete context, validation steps, and error handling patterns
- Execute with `/execute-prp` command for end-to-end implementation

#### Examples-Driven Development
- Place relevant code patterns in `examples/` folder
- Reference specific files and patterns in INITIAL.md requests
- Show both successful patterns and edge cases

### Testing & Validation
- **Data validation**: Check for expected row counts and store coverage
- **Output verification**: Validate CSV/Parquet exports contain expected columns
- **Regex testing**: Verify price, size, and THC extraction patterns work correctly

## Common Commands

```bash
# Install Playwright
uv pip install playwright
playwright install

# Run scraping notebook
# Execute cells in order, each scraper handles one product category

# Data export validation
# Check generated CSV/Parquet files for completeness
```

## Important Patterns to Follow

1. **Async function signatures**: All scraping functions should be async
2. **Random delays**: Always include randomized timeouts between operations
3. **Deduplication keys**: Use composite keys for preventing duplicate records
4. **Fallback extraction**: Try card-level extraction first, then PDP if needed
5. **Proper cleanup**: Always close pages/browsers in finally blocks or context managers
6. **State management**: Use global variables for DataFrame storage in notebooks

## Context Engineering Rules

- **Always reference examples**: Point to specific code patterns in the examples folder
- **Include validation steps**: PRPs must include data validation and testing requirements
- **Document extraction patterns**: Explain regex patterns and DOM selection strategies
- **Handle edge cases**: Account for missing data, rate limits, and site changes