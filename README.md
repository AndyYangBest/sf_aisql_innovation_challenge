<div align="center">
  <img src="frontend/public/white-theme.svg" alt="SCRAT Logo" width="200"/>

  # SCRAT - Snowflake AI SQL Innovation Challenge

  Snowflake AI-powered data analysis and visualization platform with automated EDA, collaborative dashboards, and comprehensive Cortex AI SQL functions.
</div>

## Project Overview

This project demonstrates advanced usage of Snowflake Cortex AI SQL functions across three diverse datasets:
- **Finance**: China A-Shares stock market data (1990-2023)
- **Travel**: Flight itineraries and pricing data (82M+ records)
- **Retail**: Fashion product catalog

## Python Environment Setup

This project uses Python 3.12 with uv for package management.

### Activate the virtual environment

```bash
source .venv/bin/activate
```

### Install dependencies

```bash
uv pip install -r requirements.txt
```

### Deactivate the environment

```bash
deactivate
```

## Data Infrastructure

### Database Architecture

Three databases following standardized naming convention:

| Database | Schema | Table | Description | Records |
|----------|--------|-------|-------------|---------|
| `FINANCE_DB` | PUBLIC | `CHINA_A_SHARES_STOCK_DATA` | Historical Chinese stock market data | ~85K |
| `TRAVEL_DB` | PUBLIC | `FLIGHT_ITINERARIES` | Flight booking and pricing data | 82M+ |
| `RETAIL_DB` | PUBLIC | `FASHION_PRODUCTS` | Fashion product catalog | Variable |

### Data Upload

Navigate to the `data/` directory:

```bash
cd data
```

**Setup databases:**
```bash
# Create all three databases
python snowflake_setup.py --warehouse AI_SQL_COMP --database FINANCE_DB
python snowflake_setup.py --warehouse AI_SQL_COMP --database TRAVEL_DB
python snowflake_setup.py --warehouse AI_SQL_COMP --database RETAIL_DB
```

**Upload datasets:**
```bash
# Finance data (China A-Shares)
python upload_china_stocks.py

# Retail data (Fashion products)
python universal_uploader.py --file dataset/scraped_data_2025-08-28/stylesourcebook_2025-08-28_26bf9c34_INDEX.json \
  --database RETAIL_DB --table FASHION_PRODUCTS --if-exists replace

# Travel data (Large CSV - 29GB, 82M rows)
python upload_large_csv.py
```

**Verify uploads:**
```python
python -c "
import snowflake.connector
import configparser

config = configparser.ConfigParser()
config.read('snowflake_config.ini')
p = dict(config['andy_new_account'])

conn = snowflake.connector.connect(
    user=p['user'], password=p['password'], account=p['account'],
    warehouse='AI_SQL_COMP', role='ACCOUNTADMIN'
)

cursor = conn.cursor()
for db, table in [('FINANCE_DB', 'CHINA_A_SHARES_STOCK_DATA'),
                   ('TRAVEL_DB', 'FLIGHT_ITINERARIES'),
                   ('RETAIL_DB', 'FASHION_PRODUCTS')]:
    cursor.execute(f'USE DATABASE {db}')
    result = cursor.execute(f'SELECT COUNT(*) FROM {table}').fetchone()
    print(f'{db}.{table}: {result[0]:,} rows')
"
```

## API Documentation

### Core FastAPI Application

Navigate to the `core/` directory and start the server:

```bash
cd core/src
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Access the API documentation at: `http://localhost:8000/docs`

### AI SQL Endpoints

All Snowflake Cortex AI SQL functions are exposed as REST API endpoints under `/api/v1/ai-sql/`:

#### 1. AI_COMPLETE - LLM Text Generation
```bash
POST /api/v1/ai-sql/complete
```
**Request:**
```json
{
  "model": "claude-3-7-sonnet",
  "prompt": "Analyze this financial data and provide insights",
  "response_format": null
}
```

#### 2. AI_TRANSCRIBE - Audio to Text
```bash
POST /api/v1/ai-sql/transcribe
```
**Request:**
```json
{
  "audio_file_column": "audio_file",
  "table_name": "call_recordings"
}
```

#### 3. AI_CLASSIFY - Multi-label Classification
```bash
POST /api/v1/ai-sql/classify
```
**Request:**
```json
{
  "content_column": "review_text",
  "categories": ["positive", "negative", "neutral"],
  "table_name": "FINANCE_DB.PUBLIC.CHINA_A_SHARES_STOCK_DATA",
  "prompt_prefix": "Classify the sentiment of this review: "
}
```

#### 4. AI_FILTER - Intelligent Filtering
```bash
POST /api/v1/ai-sql/filter
```
**Request:**
```json
{
  "filter_condition": "Is this stock from the technology sector? {0}",
  "table_name": "FINANCE_DB.PUBLIC.CHINA_A_SHARES_STOCK_DATA",
  "columns": ["STOCK_CODE", "STOCK_NAME_ENGLISH", "INDUSTRY_CATEGORY"]
}
```

#### 5. AI_AGG - AI-Powered Aggregation
```bash
POST /api/v1/ai-sql/aggregate
```
**Request:**
```json
{
  "column_to_aggregate": "INDUSTRY_CATEGORY",
  "aggregation_prompt": "Summarize the main industries and their characteristics",
  "table_name": "FINANCE_DB.PUBLIC.CHINA_A_SHARES_STOCK_DATA",
  "group_by": "PROVINCE"
}
```

#### 6. AI_SENTIMENT - Sentiment Analysis
```bash
POST /api/v1/ai-sql/sentiment
```
**Request:**
```json
{
  "text_column": "customer_feedback",
  "table_name": "reviews"
}
```

#### 7. SUMMARIZE - Text Summarization
```bash
POST /api/v1/ai-sql/summarize
```
**Request:**
```json
{
  "text_column": "article_content",
  "table_name": "news_articles"
}
```

#### 8. Semantic JOIN - AI-Powered Table Joining
```bash
POST /api/v1/ai-sql/semantic-join
```
**Request:**
```json
{
  "left_table": "customer_issues",
  "right_table": "solution_articles",
  "left_column": "issue_description",
  "right_column": "solution_text",
  "join_condition": "Can solution {1} address issue {0}?"
}
```

#### 9. Extract Structured Data
```bash
POST /api/v1/ai-sql/extract-structured
```
**Request:**
```json
{
  "text_column": "email_body",
  "table_name": "customer_emails",
  "extraction_prompt": "Extract customer contact information",
  "schema": {
    "type": "object",
    "properties": {
      "name": {"type": "string"},
      "email": {"type": "string"},
      "phone": {"type": "string"}
    }
  }
}
```

## Example Use Cases

### Finance: Stock Market Analysis
```python
import requests

# Classify stocks by industry using AI
response = requests.post("http://localhost:8000/api/v1/ai-sql/classify", json={
    "content_column": "INDUSTRY_CATEGORY",
    "categories": ["Technology", "Finance", "Manufacturing", "Healthcare", "Energy"],
    "table_name": "FINANCE_DB.PUBLIC.CHINA_A_SHARES_STOCK_DATA",
    "prompt_prefix": "Classify this industry: "
})
```

### Travel: Flight Route Insights
```python
# Aggregate insights about popular routes
response = requests.post("http://localhost:8000/api/v1/ai-sql/aggregate", json={
    "column_to_aggregate": "DESTINATIONAIRPORT",
    "aggregation_prompt": "Identify the top destinations and average pricing trends",
    "table_name": "TRAVEL_DB.PUBLIC.FLIGHT_ITINERARIES",
    "group_by": "STARTINGAIRPORT"
})
```

### Retail: Product Analysis
```python
# Use AI to filter fashion products
response = requests.post("http://localhost:8000/api/v1/ai-sql/filter", json={
    "filter_condition": "Is this a winter clothing item? {0}",
    "table_name": "RETAIL_DB.PUBLIC.FASHION_PRODUCTS",
    "columns": ["product_name", "category", "description"]
})
```

## Testing

Run the comprehensive test suite:

```bash
cd core
pytest tests/ -v
```

Run specific test modules:
```bash
# Test AI SQL endpoints
pytest tests/test_ai_sql.py -v

# Test user endpoints
pytest tests/test_user.py -v
```

## Architecture

### Services Layer
- `SnowflakeService`: Database connection and query execution
- `AISQLService`: Comprehensive Cortex AI SQL functions wrapper
- `EDAService`: Exploratory data analysis utilities
- `ChartService`: Visualization generation

### API Layer
- FastAPI with automatic OpenAPI documentation
- Rate limiting and caching
- Background task processing with ARQ
- Comprehensive error handling

### Database Layer
- Snowflake as primary data warehouse
- Three domain-specific databases
- Standardized naming conventions
- Metadata storage in PostgreSQL/SQLite

## Configuration

Edit `core/src/app/core/config.py` or set environment variables:

```python
# Snowflake Configuration
SNOWFLAKE_ACCOUNT = "YOUR_ACCOUNT"
SNOWFLAKE_USER = "YOUR_USER"
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD"
SNOWFLAKE_WAREHOUSE = "AI_SQL_COMP"
SNOWFLAKE_DATABASE = "AI_SQL_COMP"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"
```

## Project Structure

```
sf_aisql_innovation_challenge/
├── core/                      # FastAPI application
│   ├── src/
│   │   └── app/
│   │       ├── api/          # API endpoints
│   │       │   └── v1/
│   │       │       ├── ai_sql.py        # AI SQL endpoints
│   │       │       ├── health.py
│   │       │       └── tasks.py
│   │       ├── services/     # Business logic
│   │       │   ├── ai_sql_service.py    # Cortex AI functions
│   │       │   ├── snowflake_service.py
│   │       │   ├── eda_service.py
│   │       │   └── chart_service.py
│   │       ├── schemas/      # Pydantic models
│   │       │   ├── ai_sql.py            # AI SQL schemas
│   │       │   └── job.py
│   │       └── core/         # Core utilities
│   └── tests/                # Test suite
│       ├── test_ai_sql.py               # AI SQL tests
│       └── test_user.py
├── data/                      # Data management
│   ├── dataset/              # Raw datasets
│   ├── snowflake_setup.py    # Database creation
│   ├── universal_uploader.py # General purpose uploader
│   ├── upload_large_csv.py   # Large file handler
│   └── snowflake_config.ini  # Credentials
└── examples/                  # Snowflake AI SQL examples
    ├── sfguide-getting-started-with-cortex-aisql/
    ├── sfguide-building-cortex-aisql-powered-callcentre-analytics/
    └── sfguide-customer-issue-deduplication-demo/
```

## Key Features

✅ **Comprehensive AI SQL Coverage**: All Cortex functions (AI_COMPLETE, AI_TRANSCRIBE, AI_CLASSIFY, AI_FILTER, AI_AGG, AI_SENTIMENT, SUMMARIZE)

✅ **Production-Ready APIs**: FastAPI with rate limiting, caching, async support

✅ **Multi-Domain Data**: Finance, Travel, and Retail datasets

✅ **Scalable Architecture**: Handles datasets from KBs to 29GB

✅ **Complete Testing**: Comprehensive unit tests for all endpoints

✅ **Standard Naming**: Consistent database and table naming conventions

✅ **Reusable Tools**: Universal uploader for CSV/Excel/JSON files

## Contributing

This project follows best practices:
- Type hints throughout
- Comprehensive docstrings
- Unit tests for all endpoints
- Async/await patterns
- Minimal code changes (reuse existing tools)

## License

MIT
