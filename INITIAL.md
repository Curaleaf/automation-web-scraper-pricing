## FEATURE:

This is a Context Engineering template repository for a web scraping automation, specifically focused on pricing data extraction from dispensary websites using Playwright and Python.

Build an automation framework that can scrapes pricing data extraction from dispensary websites.
The automation process will read the pricing data for the listed subcategory listed and save each subcategory dataframe into a Snowflake database.

- run each subcategory in parallel mode.
- Store code-base in Github repo
- Use Github workflow actions for execution of the web scraping automation process.
- Use Pydantic AI agent and data framework to orchestra the following 
    - extract pricing data by category, 
    - save pricing data by category in Snowflake database

Include test scripts to test each agent. 

## EXAMPLES:

Please read the examples\CLAUDE.md for architect of automation

## DOCUMENTATION:

BASE = "https://www.trulieve.com"
DISPENSARIES_URL = f"{BASE}/dispensaries"
CATEGORY_URL = f"{BASE}/category/flower/whole-flower"
SUBCATEGORY   = "Whole Flower"
OUT_PREFIX    = "trulieve_FL_whole_flower"

BASE = "https://www.trulieve.com"
DISPENSARIES_URL = f"{BASE}/dispensaries"
CATEGORY_URL = f"{BASE}/category/flower/minis"
SUBCATEGORY  = "Ground & Shake"
OUT_PREFIX   = "trulieve_FL_ground_shake"

BASE = "https://www.trulieve.com"
DISPENSARIES_URL = f"{BASE}/dispensaries"
CATEGORY_URL = f"{BASE}/category/flower/pre-rolls"
SUBCATEGORY  = "Pre-Rolls"
OUT_PREFIX   = "trulieve_FL_pre_rolls"

BASE = "https://www.trulieve.com"
DISPENSARIES_URL = f"{BASE}/dispensaries"
CATEGORY_URL = f"{BASE}/category/flower/ground-shake"
SUBCATEGORY  = "Ground & Shake"
OUT_PREFIX   = "trulieve_FL_ground_shake"

Snowflake API documentation: https://docs.snowflake.com/en/developer-guide/sql-api/index
Snowflake Instance - CURALEAF-CURAPROD.snowflakecomputing.com

Database
    SANDBOX_EDW
Schema
    ANALYTICS
Table type
    Static
Table name
    TL_Scrape_WHOLE_FLOWER
    TL_Scrape_Pre_Rolls
    TL_Scrape_Ground_Shake

- Pydantic AI Official Documentation: https://ai.pydantic.dev/
- Agent Creation Guide: https://ai.pydantic.dev/agents/
- Tool Integration: https://ai.pydantic.dev/tools/
- Testing Patterns: https://ai.pydantic.dev/testing/
- Model Providers: https://ai.pydantic.dev/models/


## OTHER CONSIDERATIONS:
- Include a .env.example, README with instructions for setup
- Include the project structure in the README.
- Virtual environment has already been set up with the necessary dependencies.
- Use python_dotenv and load_env() for environment variables

