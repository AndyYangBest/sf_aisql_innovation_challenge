# AI Prompts Configuration

This directory contains centralized AI prompt templates used throughout the application.

## Structure

```
core/src/app/config/
├── prompts.py          # All AI prompt templates
└── README.md           # This file
```

## How to Modify Prompts

### 1. **Editing Existing Prompts**

All prompts are stored as constants in `prompts.py`. To modify a prompt:

1. Open `core/src/app/config/prompts.py`
2. Find the prompt constant (e.g., `SUGGEST_METADATA_PROMPT`)
3. Edit the template string
4. Save and restart the backend server

**Example:**
```python
# Before
SUGGEST_METADATA_PROMPT = """Analyze this SQL query..."""

# After
SUGGEST_METADATA_PROMPT = """[Your improved prompt here]..."""
```

### 2. **Adding New Prompts**

To add a new prompt:

1. Add a new constant in `prompts.py`
2. Use `{variable_name}` for variables that will be substituted
3. Create a helper function if needed for complex variable formatting

**Example:**
```python
# In prompts.py
NEW_ANALYSIS_PROMPT = """Analyze this data:

Data: {data}
Context: {context}

Provide insights about:
1. {aspect1}
2. {aspect2}
"""

# Helper function
def build_analysis_prompt(data: str, context: str, aspects: list[str]) -> str:
    return NEW_ANALYSIS_PROMPT.format(
        data=data,
        context=context,
        aspect1=aspects[0],
        aspect2=aspects[1],
    )
```

### 3. **Using Prompts in Code**

Prompts are imported and used through helper classes in `services/ai_helpers.py`:

```python
from app.config.prompts import build_metadata_prompt
from app.services.ai_helpers import AIMetadataGenerator

# In your endpoint
generator = AIMetadataGenerator(ai_service)
metadata = await generator.suggest_table_metadata(sql, columns, rows)
```

## Available Prompts

### Metadata Generation

**`SUGGEST_METADATA_PROMPT`**
- **Purpose**: Generate table metadata (name, tags, summary, use cases) from SQL + data
- **Variables**: `{sql_query}`, `{columns_section}`, `{sample_data_section}`
- **Used in**: `/api/v1/ai-sql/suggest-metadata`
- **Helper**: `build_metadata_prompt()`

### Data Analysis (Coming Soon)

**`ANALYZE_TABLE_STRUCTURE_PROMPT`**
- **Purpose**: Analyze table structure and data quality
- **Variables**: `{table_name}`, `{columns}`, `{sample_data}`
- **Status**: Template defined, implementation pending

**`GENERATE_SQL_FROM_DESCRIPTION_PROMPT`**
- **Purpose**: Generate SQL from natural language
- **Variables**: `{description}`, `{available_tables}`, `{schema}`
- **Status**: Template defined, implementation pending

### Insight Generation (Coming Soon)

**`GENERATE_INSIGHTS_PROMPT`**
- **Purpose**: Generate business insights from data
- **Variables**: `{data_summary}`, `{column_stats}`
- **Status**: Template defined, implementation pending

## Best Practices

### 1. **Clear Instructions**
Always provide clear, specific instructions to the AI:
```python
# Good
"Generate a table name (2-4 words, snake_case) based on the data content"

# Bad
"Make a name"
```

### 2. **Structured Output**
Request structured output (JSON) with exact format:
```python
"""
Respond ONLY with valid JSON in this exact format:
{
    "field1": "value",
    "field2": ["item1", "item2"]
}
"""
```

### 3. **Context is Key**
Include relevant context (SQL, columns, sample data) for better results:
```python
prompt = build_metadata_prompt(
    sql=query,
    columns=column_definitions,  # Include this!
    sample_rows=first_5_rows,    # And this!
)
```

### 4. **Variable Naming**
Use descriptive variable names in templates:
```python
# Good
{sql_query}, {column_definitions}, {sample_data}

# Bad
{q}, {c}, {d}
```

## Testing Prompts

### 1. **Local Testing**
Modify prompt → Restart server → Test endpoint:
```bash
# Restart backend
cd core
uv run uvicorn app.main:app --reload

# Test endpoint
curl -X POST http://localhost:8000/api/v1/ai-sql/suggest-metadata \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM products", "columns": [...], "sample_rows": [...]}'
```

### 2. **Check Backend Logs**
Backend prints AI responses for debugging:
```
[AI Metadata] Calling AI with enhanced prompt
[AI Metadata] AI Response: {"table_name": "..."}
[AI Metadata] Successfully generated AI metadata
```

### 3. **Fallback Testing**
To test the fallback logic, temporarily break the AI call or disable Snowflake Cortex.

## Troubleshooting

### Prompt Not Working?

1. **Check Format**
   - Ensure JSON format is correct
   - Verify all variables are being substituted

2. **Check Backend Logs**
   - Look for `[AI Metadata]` logs
   - Check for parsing errors

3. **Verify Model**
   - Ensure `mistral-large2` is available in Snowflake Cortex
   - Try different model if needed

4. **Test Fallback**
   - Fallback should always work even if AI fails
   - Check `smart_metadata_fallback()` in `ai_helpers.py`

## Contributing

When adding new AI features:

1. Add prompt template to `prompts.py`
2. Create helper function if needed
3. Create/update helper class in `ai_helpers.py`
4. Use helper in API endpoint
5. Update this README

## Examples

### Example 1: Improving Metadata Prompt

**Goal**: Make AI generate more business-focused summaries

**Change in `prompts.py`:**
```python
# Change instruction #3
"3. A brief summary (1-2 sentences) describing what business insights this data provides and its strategic value"
```

### Example 2: Adding Confidence Scores

**Goal**: Get AI to rate its confidence

**Change in `prompts.py`:**
```python
SUGGEST_METADATA_PROMPT = """...

Respond ONLY with valid JSON in this exact format:
{
    "table_name": "descriptive_name_here",
    "tags": ["tag1", "tag2", "tag3"],
    "summary": "Clear description...",
    "use_cases": ["Use case 1", "Use case 2"],
    "confidence": 0.95  # ADD THIS
}"""
```

**Update response model in `ai_sql.py`:**
```python
class SuggestMetadataResponse(BaseModel):
    # ... existing fields ...
    confidence: float | None = None  # ADD THIS
```

---

**For more information, see:**
- `core/src/app/services/ai_helpers.py` - AI helper classes
- `core/src/app/api/v1/ai_sql.py` - API endpoints using prompts
