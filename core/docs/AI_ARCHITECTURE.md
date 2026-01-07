# AI Services Architecture

This document describes the architecture for AI-powered features in the application.

## Overview

The AI services are organized into three layers:

```
┌─────────────────────────────────────┐
│   API Endpoints (ai_sql.py)        │  ← User-facing REST APIs
├─────────────────────────────────────┤
│   AI Helpers (ai_helpers.py)       │  ← Reusable business logic
├─────────────────────────────────────┤
│   Prompts (config/prompts.py)      │  ← Centralized prompt templates
├─────────────────────────────────────┤
│   AI Service (ModularAISQLService) │  ← Snowflake Cortex integration
└─────────────────────────────────────┘
```

## Directory Structure

```
core/src/app/
├── api/v1/
│   └── ai_sql.py                    # API endpoints
├── config/
│   ├── __init__.py                  # Package exports
│   ├── prompts.py                   # Prompt templates ⭐
│   └── README.md                    # Prompt documentation
├── services/
│   ├── ai_helpers.py                # AI helper classes ⭐
│   ├── modular_ai_sql_service.py   # Snowflake Cortex wrapper
│   └── snowflake_service.py        # Snowflake connection
└── schemas/
    └── ai_sql.py                    # Request/Response models
```

## Layer Details

### 1. Prompts Layer (`config/prompts.py`)

**Purpose**: Centralized storage of all AI prompts

**Key Features**:
- All prompts in one file for easy modification
- Template variables using `{variable}` syntax
- Helper functions for complex formatting
- No business logic, just templates

**Example**:
```python
# Define prompt template
SUGGEST_METADATA_PROMPT = """Analyze this SQL query...
SQL: {sql_query}
Columns: {columns_section}
..."""

# Helper to build prompt
def build_metadata_prompt(sql, columns, sample_rows):
    return SUGGEST_METADATA_PROMPT.format(
        sql_query=sql,
        columns_section=build_columns_section(columns),
        ...
    )
```

**To modify prompts**: Edit `config/prompts.py` → Restart server

### 2. AI Helpers Layer (`services/ai_helpers.py`)

**Purpose**: Reusable AI-powered functionality

**Key Classes**:
- `AIMetadataGenerator`: Generate table metadata
- `AIDataAnalyzer`: Analyze data quality (future)
- `AIInsightGenerator`: Generate insights (future)

**Key Features**:
- Uses prompts from config layer
- Handles AI service calls
- Parses and validates responses
- Provides smart fallbacks

**Example**:
```python
class AIMetadataGenerator:
    async def suggest_table_metadata(self, sql, columns, sample_rows):
        # Build prompt from template
        prompt = build_metadata_prompt(sql, columns, sample_rows)

        # Call AI service
        response = await self.ai_service.ai_complete(...)

        # Parse and validate
        metadata = json.loads(response)
        return metadata
```

**Benefits**:
- DRY principle (Don't Repeat Yourself)
- Easy to test
- Easy to extend with new AI features

### 3. API Layer (`api/v1/ai_sql.py`)

**Purpose**: REST API endpoints for AI features

**Key Features**:
- FastAPI endpoints
- Request/Response validation
- Error handling
- Logging

**Example**:
```python
@router.post("/suggest-metadata")
async def suggest_metadata(request, ai_service):
    # Initialize helper
    generator = AIMetadataGenerator(ai_service)

    # Use helper to generate metadata
    metadata = await generator.suggest_table_metadata(...)

    # Return response
    return SuggestMetadataResponse(...)
```

**Benefits**:
- Thin endpoints (business logic in helpers)
- Consistent error handling
- Easy to add new endpoints

## Data Flow

### Example: Generate Metadata

```
1. Frontend sends POST to /api/v1/ai-sql/suggest-metadata
   ↓
2. API endpoint validates request
   ↓
3. Creates AIMetadataGenerator instance
   ↓
4. Generator builds prompt using config/prompts.py
   ↓
5. Generator calls ModularAISQLService.ai_complete()
   ↓
6. Service calls Snowflake Cortex AI_COMPLETE
   ↓
7. AI returns JSON response
   ↓
8. Generator parses and validates response
   ↓
9. API endpoint returns formatted response
   ↓
10. Frontend receives metadata
```

## Adding New AI Features

### Step 1: Add Prompt Template

**File**: `config/prompts.py`

```python
# Add new prompt constant
NEW_FEATURE_PROMPT = """Do something with this data:
{data}
{context}

Provide: {instructions}
"""

# Add helper function
def build_new_feature_prompt(data, context, instructions):
    return NEW_FEATURE_PROMPT.format(...)
```

### Step 2: Create Helper Class

**File**: `services/ai_helpers.py`

```python
class AINewFeature:
    def __init__(self, ai_service):
        self.ai_service = ai_service

    async def process(self, data, context):
        # Build prompt
        prompt = build_new_feature_prompt(data, context, ...)

        # Call AI
        response = await self.ai_service.ai_complete(...)

        # Parse and return
        return json.loads(response)
```

### Step 3: Add API Endpoint

**File**: `api/v1/ai_sql.py`

```python
@router.post("/new-feature")
async def new_feature(request, ai_service):
    helper = AINewFeature(ai_service)
    result = await helper.process(...)
    return NewFeatureResponse(...)
```

### Step 4: Update Frontend

**File**: `frontend/src/api/tables.ts`

```typescript
async newFeature(data): Promise<ApiResponse<...>> {
  return apiRequest(async () => {
    const response = await fetch('/api/v1/ai-sql/new-feature', {
      method: 'POST',
      body: JSON.stringify({ data }),
    });
    return await response.json();
  });
}
```

## Benefits of This Architecture

### 1. **Separation of Concerns**
- Prompts: What to ask AI
- Helpers: How to use AI
- APIs: How to expose AI features

### 2. **Easy Maintenance**
- Modify prompts without touching code
- Update business logic without changing APIs
- Add features without breaking existing ones

### 3. **Testability**
- Test prompts independently
- Test helpers with mock AI service
- Test APIs with mock helpers

### 4. **Reusability**
- Use same helper in multiple endpoints
- Use same prompt in multiple contexts
- Share code across features

### 5. **Scalability**
- Add new AI features quickly
- Extend existing features easily
- Support multiple AI models

## Testing

### Test Prompts
```python
# In tests/test_prompts.py
def test_metadata_prompt():
    prompt = build_metadata_prompt(
        sql="SELECT * FROM users",
        columns=[{"name": "id", "type": "INT"}],
        sample_rows=[{"id": 1}]
    )
    assert "SELECT * FROM users" in prompt
    assert "id (INT)" in prompt
```

### Test Helpers
```python
# In tests/test_ai_helpers.py
@pytest.mark.asyncio
async def test_metadata_generator(mock_ai_service):
    generator = AIMetadataGenerator(mock_ai_service)
    metadata = await generator.suggest_table_metadata(...)
    assert metadata["table_name"] == "expected_name"
```

### Test APIs
```python
# In tests/test_ai_sql.py
@pytest.mark.asyncio
async def test_suggest_metadata_endpoint(client):
    response = await client.post("/api/v1/ai-sql/suggest-metadata", ...)
    assert response.status_code == 200
    assert "suggested_name" in response.json()
```

## Monitoring

### Backend Logs
All AI operations log to console with `[AI Metadata]` prefix:

```
[AI Metadata] Generating suggestions for SQL: SELECT * FROM...
[AI Metadata] Columns: 15
[AI Metadata] Sample rows: 5
[AI Metadata] Successfully generated AI metadata
[AI Metadata] Name: product_catalog, Tags: ['products', 'catalog']
```

### Error Handling
- AI failures automatically fall back to smart SQL parsing
- All errors are logged with full stack traces
- Fallback ensures feature always works

## Future Enhancements

### Planned Features
1. **Data Quality Analysis**: Analyze data quality and suggest improvements
2. **SQL Generation**: Generate SQL from natural language
3. **Business Insights**: Extract business insights from data
4. **Anomaly Detection**: Detect data anomalies
5. **Schema Suggestions**: Suggest optimal schema designs

### Extensibility
The architecture supports:
- Multiple AI models (Claude, Mistral, etc.)
- Custom prompt templates per use case
- A/B testing different prompts
- Caching AI responses
- Rate limiting per feature

## Resources

- **Prompts**: `core/src/app/config/prompts.py`
- **Helpers**: `core/src/app/services/ai_helpers.py`
- **APIs**: `core/src/app/api/v1/ai_sql.py`
- **Documentation**: `core/src/app/config/README.md`
