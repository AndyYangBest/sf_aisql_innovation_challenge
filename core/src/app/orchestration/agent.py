"""AI SQL Agent - Unified orchestration entry point.

This module provides a conversational agent that can understand natural language
requests and route them to appropriate AI SQL tools.
"""

import json
from typing import Any

from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService
from .tools import AISQLToolkit, ToolCategory, ToolDefinition


class AISQLAgent:
    """Conversational agent for AI SQL operations.

    This agent provides a unified entry point for natural language interactions
    with AI SQL capabilities. It can:
    - Understand user intent from natural language
    - Route requests to appropriate tools
    - Execute AI SQL operations
    - Return formatted results

    Usage:
        agent = AISQLAgent(snowflake_service)
        result = await agent.process("Analyze sentiment of customer reviews in the reviews table")
    """

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        llm_model: str = "claude-3-7-sonnet",
    ):
        """Initialize agent with Snowflake service.

        Args:
            snowflake_service: Snowflake service instance
            llm_model: LLM model to use for intent understanding
        """
        self.sf_service = snowflake_service
        self.ai_sql_service = ModularAISQLService(snowflake_service)
        self.toolkit = AISQLToolkit(self.ai_sql_service)
        self.llm_model = llm_model
        self.conversation_history: list[dict[str, str]] = []

    async def process(
        self,
        user_request: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Process a natural language request.

        Args:
            user_request: Natural language request from user
            context: Optional context (table names, columns, etc.)

        Returns:
            Dictionary containing:
                - tool_used: Name of the tool that was used
                - sql_generated: SQL query that was generated
                - results: Query results
                - summary: Human-readable summary
        """
        # Add to conversation history
        self.conversation_history.append({"role": "user", "content": user_request})

        # Step 1: Understand intent and select tool
        tool_selection = await self._select_tool(user_request, context)

        if tool_selection["error"]:
            return {
                "error": tool_selection["error"],
                "suggestion": tool_selection.get("suggestion"),
            }

        # Step 2: Extract parameters from request
        parameters = await self._extract_parameters(
            user_request, tool_selection["tool"], context
        )

        # Step 3: Execute the tool
        try:
            tool = self.toolkit.get_tool(tool_selection["tool"])
            if not tool:
                return {"error": f"Tool '{tool_selection['tool']}' not found"}

            results = await tool.function(**parameters)

            # Step 4: Generate summary
            summary = await self._generate_summary(
                user_request, tool_selection["tool"], results
            )

            response = {
                "tool_used": tool_selection["tool"],
                "parameters": parameters,
                "results": results,
                "summary": summary,
                "sql_preview": tool_selection.get("sql_preview"),
            }

            # Add to conversation history
            self.conversation_history.append(
                {"role": "assistant", "content": json.dumps(response, indent=2)}
            )

            return response

        except Exception as e:
            error_response = {
                "error": str(e),
                "tool_attempted": tool_selection["tool"],
                "parameters": parameters,
            }
            self.conversation_history.append(
                {"role": "assistant", "content": json.dumps(error_response, indent=2)}
            )
            return error_response

    async def _select_tool(
        self, user_request: str, context: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Select the most appropriate tool for the request.

        Uses LLM to understand intent and map to available tools.
        """
        # Build tool selection prompt
        tools_info = self._build_tools_description()
        context_info = json.dumps(context, indent=2) if context else "No context provided"

        prompt = f"""You are an AI SQL assistant. Analyze the user's request and select the most appropriate tool.

Available Tools:
{tools_info}

User Request: {user_request}

Context: {context_info}

Respond with JSON in this format:
{{
    "tool": "tool_name",
    "reasoning": "why this tool is appropriate",
    "confidence": 0.95,
    "sql_preview": "brief description of what SQL will be generated"
}}

If the request is unclear or no tool matches, respond with:
{{
    "error": "description of the problem",
    "suggestion": "what information is needed"
}}
"""

        # Use AI_COMPLETE to select tool
        response = await self.ai_sql_service.ai_complete(
            model=self.llm_model,
            prompt=prompt,
            response_format={
                "type": "object",
                "properties": {
                    "tool": {"type": "string"},
                    "reasoning": {"type": "string"},
                    "confidence": {"type": "number"},
                    "sql_preview": {"type": "string"},
                    "error": {"type": "string"},
                    "suggestion": {"type": "string"},
                },
            },
        )

        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return {
                "error": "Failed to parse tool selection response",
                "raw_response": response,
            }

    async def _extract_parameters(
        self,
        user_request: str,
        tool_name: str,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Extract parameters needed for the selected tool.

        Uses LLM to extract structured parameters from natural language.
        """
        tool = self.toolkit.get_tool(tool_name)
        if not tool:
            return {}

        context_info = json.dumps(context, indent=2) if context else "No context provided"

        prompt = f"""Extract parameters for the '{tool_name}' tool from the user's request.

Tool: {tool.name}
Description: {tool.description}

Required Parameters:
{json.dumps(tool.parameters, indent=2)}

User Request: {user_request}

Context: {context_info}

Extract the parameters and respond with JSON. Use the context to fill in missing information.
If a parameter is not mentioned and not in context, use a reasonable default or null.

Example response format:
{{
    "table_name": "reviews",
    "text_column": "review_text",
    "categories": ["positive", "negative", "neutral"]
}}
"""

        response = await self.ai_sql_service.ai_complete(
            model=self.llm_model, prompt=prompt
        )

        try:
            return json.loads(response)
        except json.JSONDecodeError:
            # Fallback: try to extract from context
            return context or {}

    async def _generate_summary(
        self, user_request: str, tool_used: str, results: Any
    ) -> str:
        """Generate a human-readable summary of the results.

        Uses LLM to create a natural language summary.
        """
        # Limit results for summary (avoid token limits)
        results_preview = str(results)[:2000]

        prompt = f"""Generate a concise, human-readable summary of these AI SQL results.

User's Original Request: {user_request}

Tool Used: {tool_used}

Results Preview:
{results_preview}

Provide a 2-3 sentence summary that:
1. Confirms what was done
2. Highlights key findings or insights
3. Suggests next steps if relevant

Keep it conversational and helpful.
"""

        summary = await self.ai_sql_service.ai_complete(
            model=self.llm_model, prompt=prompt
        )

        return summary.strip()

    def _build_tools_description(self) -> str:
        """Build a formatted description of all available tools."""
        tools_by_category = {}
        for tool in self.toolkit.get_all_tools():
            if tool.category not in tools_by_category:
                tools_by_category[tool.category] = []
            tools_by_category[tool.category].append(tool)

        description_parts = []
        for category, tools in tools_by_category.items():
            description_parts.append(f"\n{category.value.upper()}:")
            for tool in tools:
                description_parts.append(f"  - {tool.name}: {tool.description}")
                description_parts.append(f"    Examples: {', '.join(tool.examples[:2])}")

        return "\n".join(description_parts)

    def get_conversation_history(self) -> list[dict[str, str]]:
        """Get the conversation history."""
        return self.conversation_history

    def clear_conversation_history(self):
        """Clear the conversation history."""
        self.conversation_history = []

    async def suggest_next_actions(self, current_results: Any) -> list[str]:
        """Suggest possible next actions based on current results.

        Uses LLM to analyze results and suggest follow-up operations.
        """
        results_preview = str(current_results)[:1000]

        prompt = f"""Based on these AI SQL results, suggest 3-5 logical next actions the user might want to take.

Current Results:
{results_preview}

Available Tool Categories:
{', '.join([cat.value for cat in ToolCategory])}

Suggest specific, actionable next steps. Format as a JSON array of strings.
Example: ["Classify the results into categories", "Generate a summary", "Translate to Spanish"]
"""

        response = await self.ai_sql_service.ai_complete(
            model=self.llm_model,
            prompt=prompt,
            response_format={
                "type": "object",
                "properties": {"suggestions": {"type": "array", "items": {"type": "string"}}},
            },
        )

        try:
            parsed = json.loads(response)
            return parsed.get("suggestions", [])
        except json.JSONDecodeError:
            return []

    def get_available_tools(self) -> dict[str, Any]:
        """Get information about all available tools."""
        return self.toolkit.to_dict()

    def get_tool_by_category(self, category: str) -> list[dict[str, Any]]:
        """Get tools filtered by category."""
        try:
            cat_enum = ToolCategory(category)
            tools = self.toolkit.get_tools_by_category(cat_enum)
            return [tool.to_dict() for tool in tools]
        except ValueError:
            return []


class AgentSession:
    """Manages a stateful agent session with context persistence.

    This class maintains session state across multiple agent interactions,
    allowing for multi-turn conversations with context awareness.
    """

    def __init__(
        self,
        agent: AISQLAgent,
        session_id: str,
        initial_context: dict[str, Any] | None = None,
    ):
        """Initialize agent session.

        Args:
            agent: AISQLAgent instance
            session_id: Unique session identifier
            initial_context: Initial context (tables, schemas, etc.)
        """
        self.agent = agent
        self.session_id = session_id
        self.context = initial_context or {}
        self.results_history: list[dict[str, Any]] = []

    async def process(self, user_request: str) -> dict[str, Any]:
        """Process a request within this session.

        Automatically maintains context across requests.
        """
        result = await self.agent.process(user_request, self.context)

        # Update context with results
        if "results" in result and not result.get("error"):
            self.results_history.append(result)

            # Auto-update context with table/column info if detected
            if "parameters" in result:
                params = result["parameters"]
                if "table_name" in params:
                    self.context["last_table"] = params["table_name"]
                if "text_column" in params:
                    self.context["last_column"] = params["text_column"]

        return result

    def update_context(self, updates: dict[str, Any]):
        """Update session context."""
        self.context.update(updates)

    def get_context(self) -> dict[str, Any]:
        """Get current session context."""
        return self.context

    def get_results_history(self) -> list[dict[str, Any]]:
        """Get history of results in this session."""
        return self.results_history

    async def suggest_next(self) -> list[str]:
        """Suggest next actions based on session history."""
        if not self.results_history:
            return []

        last_result = self.results_history[-1]
        return await self.agent.suggest_next_actions(last_result.get("results"))
