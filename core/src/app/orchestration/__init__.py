"""AI SQL Orchestration Layer.

This module provides a clean, pluggable orchestration layer for AI SQL capabilities.
It wraps the core AI SQL service methods as tools that can be used by AI agents.

Available implementations:
1. Generic Agent (agent.py) - Framework-agnostic implementation
2. Strands Agent (strands_aisql_agent.py) - Native Strands Agents integration
3. Toolkit (tools.py) - Reusable tool definitions for any framework
"""

from .agent import AgentSession, AISQLAgent
from .tools import AISQLToolkit, ToolCategory, ToolDefinition

# Strands Agents integration (optional, requires: pip install strands-agents)
try:
    from .strands_aisql_agent import AISQLStrandsAgent, create_aisql_agent

    __all__ = [
        "AISQLAgent",
        "AgentSession",
        "AISQLToolkit",
        "ToolCategory",
        "ToolDefinition",
        "AISQLStrandsAgent",
        "create_aisql_agent",
    ]
except ImportError:
    __all__ = [
        "AISQLAgent",
        "AgentSession",
        "AISQLToolkit",
        "ToolCategory",
        "ToolDefinition",
    ]
