"""Configuration package for AI prompts and templates."""

from .prompts import (
    ANALYZE_TABLE_STRUCTURE_PROMPT,
    GENERATE_INSIGHTS_PROMPT,
    GENERATE_SQL_FROM_DESCRIPTION_PROMPT,
    SUGGEST_METADATA_PROMPT,
    build_columns_section,
    build_metadata_prompt,
    build_sample_data_section,
)

__all__ = [
    # Prompt templates
    "SUGGEST_METADATA_PROMPT",
    "ANALYZE_TABLE_STRUCTURE_PROMPT",
    "GENERATE_SQL_FROM_DESCRIPTION_PROMPT",
    "GENERATE_INSIGHTS_PROMPT",
    # Helper functions
    "build_metadata_prompt",
    "build_columns_section",
    "build_sample_data_section",
]
