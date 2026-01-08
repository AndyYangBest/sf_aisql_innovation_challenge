"""Strands Agents Integration for AI SQL.

This module shows how to integrate AI SQL capabilities with Strands Agents framework.
Install: pip install multi-agent-orchestrator

Usage:
    from app.orchestration.strands_integration import create_aisql_agent

    agent = create_aisql_agent(snowflake_service)
    response = await agent.process_request("Analyze sentiment of reviews", "user-123", "session-456")
"""

from typing import Any

# Strands Agents imports (install: pip install multi-agent-orchestrator)
try:
    from multi_agent_orchestrator.agents import BedrockLLMAgent, Agent, AgentResponse
    from multi_agent_orchestrator.orchestrator import MultiAgentOrchestrator
    from multi_agent_orchestrator.types import ConversationMessage

    STRANDS_AVAILABLE = True
except ImportError:
    STRANDS_AVAILABLE = False
    # Fallback types for type hints
    Agent = Any
    AgentResponse = Any
    MultiAgentOrchestrator = Any

from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService
from .tools import AISQLToolkit


class AISQLStrandsAgent:
    """AI SQL Agent integrated with Strands Agents framework.

    This agent wraps all AI SQL capabilities as Strands-compatible tools
    and provides a conversational interface using Strands orchestration.
    """

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        agent_name: str = "AI SQL Agent",
        agent_description: str = "Specialized in Snowflake Cortex AI SQL operations",
    ):
        """Initialize Strands-integrated AI SQL agent.

        Args:
            snowflake_service: Snowflake service instance
            agent_name: Name for the agent
            agent_description: Description of agent capabilities
        """
        if not STRANDS_AVAILABLE:
            raise ImportError(
                "Strands Agents not installed. Install with: pip install multi-agent-orchestrator"
            )

        self.sf_service = snowflake_service
        self.ai_sql_service = ModularAISQLService(snowflake_service)
        self.toolkit = AISQLToolkit(self.ai_sql_service)

        # Create Strands agent with AI SQL tools
        self.agent = self._create_strands_agent(agent_name, agent_description)

    def _create_strands_agent(self, name: str, description: str) -> Agent:
        """Create a Strands Agent with AI SQL tools registered.

        In Strands, tools are typically registered through the agent configuration
        or by extending the agent class with custom tool methods.
        """
        # Build comprehensive tool description for the agent
        tools_description = self._build_tools_description()

        # Create Bedrock LLM Agent (or custom agent)
        agent = BedrockLLMAgent({
            "name": name,
            "description": f"{description}\n\nAvailable Tools:\n{tools_description}",
            "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",  # or your preferred model
            "streaming": False,
            "inference_config": {
                "maxTokens": 2000,
                "temperature": 0.7,
            }
        })

        return agent

    def _build_tools_description(self) -> str:
        """Build formatted description of all AI SQL tools for the agent."""
        tools_by_category = {}
        for tool in self.toolkit.get_all_tools():
            if tool.category not in tools_by_category:
                tools_by_category[tool.category] = []
            tools_by_category[tool.category].append(tool)

        description_parts = []
        for category, tools in tools_by_category.items():
            description_parts.append(f"\n{category.value.upper()}:")
            for tool in tools:
                description_parts.append(f"  • {tool.name}: {tool.description}")
                if tool.examples:
                    description_parts.append(f"    Examples: {tool.examples[0]}")

        return "\n".join(description_parts)

    async def process_request(
        self,
        user_input: str,
        user_id: str,
        session_id: str,
        additional_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Process a user request through Strands orchestration.

        Args:
            user_input: Natural language query
            user_id: User identifier
            session_id: Session identifier
            additional_params: Additional context/parameters

        Returns:
            Dictionary with response and metadata
        """
        # In Strands, the agent processes the request and returns a response
        response = await self.agent.process_request(
            user_input,
            user_id,
            session_id,
            additional_params or {}
        )

        # Parse the agent's response to extract tool usage and execute
        result = await self._execute_tool_from_response(response, additional_params)

        return result

    async def _execute_tool_from_response(
        self,
        agent_response: AgentResponse,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Execute the appropriate AI SQL tool based on agent's response.

        The agent's response should indicate which tool to use and with what parameters.
        This method parses that and executes the actual AI SQL operation.
        """
        # Extract tool selection from agent response
        # This is a simplified version - actual implementation depends on your prompt engineering
        response_text = agent_response.output if hasattr(agent_response, 'output') else str(agent_response)

        # Parse tool name and parameters from response
        # In production, you'd use structured output or function calling
        tool_info = await self._parse_tool_selection(response_text, context)

        if tool_info.get("error"):
            return {
                "error": tool_info["error"],
                "agent_response": response_text,
            }

        # Execute the selected tool
        tool = self.toolkit.get_tool(tool_info["tool_name"])
        if not tool:
            return {
                "error": f"Tool '{tool_info['tool_name']}' not found",
                "agent_response": response_text,
            }

        try:
            results = await tool.function(**tool_info["parameters"])

            return {
                "tool_used": tool_info["tool_name"],
                "parameters": tool_info["parameters"],
                "results": results,
                "agent_response": response_text,
                "metadata": agent_response.metadata if hasattr(agent_response, 'metadata') else {},
            }
        except Exception as e:
            return {
                "error": str(e),
                "tool_attempted": tool_info["tool_name"],
                "parameters": tool_info["parameters"],
                "agent_response": response_text,
            }

    async def _parse_tool_selection(
        self,
        response_text: str,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Parse tool selection from agent response.

        In production, you'd use structured output or function calling.
        This is a simplified version for demonstration.
        """
        # Use AI to parse the response and extract tool + parameters
        import json

        parse_prompt = f"""Parse this agent response and extract the tool name and parameters.

Agent Response: {response_text}

Context: {json.dumps(context or {}, indent=2)}

Available Tools: {', '.join(self.toolkit.get_tool_names())}

Respond with JSON:
{{
    "tool_name": "tool_name",
    "parameters": {{"param1": "value1", ...}},
    "reasoning": "why this tool"
}}

If unclear, respond with: {{"error": "description"}}
"""

        parsed = await self.ai_sql_service.ai_complete(
            model="claude-3-7-sonnet",
            prompt=parse_prompt,
        )

        try:
            return json.loads(parsed)
        except json.JSONDecodeError:
            return {"error": "Failed to parse tool selection"}


class AISQLOrchestrator:
    """Multi-agent orchestrator for AI SQL operations.

    This class uses Strands' MultiAgentOrchestrator to manage multiple
    specialized AI SQL agents for different tasks.
    """

    def __init__(self, snowflake_service: SnowflakeService):
        """Initialize orchestrator with multiple AI SQL agents."""
        if not STRANDS_AVAILABLE:
            raise ImportError(
                "Strands Agents not installed. Install with: pip install multi-agent-orchestrator"
            )

        self.sf_service = snowflake_service
        self.orchestrator = MultiAgentOrchestrator()

        # Add specialized agents
        self._add_agents()

    def _add_agents(self):
        """Add specialized AI SQL agents to the orchestrator."""
        # Text Analysis Agent
        analysis_agent = BedrockLLMAgent({
            "name": "Text Analysis Agent",
            "description": """Specializes in text analysis operations:
            - Sentiment analysis
            - Classification
            - Filtering
            - Similarity detection

            Use this agent for understanding and categorizing text data.""",
            "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
        })
        self.orchestrator.add_agent(analysis_agent)

        # Data Extraction Agent
        extraction_agent = BedrockLLMAgent({
            "name": "Data Extraction Agent",
            "description": """Specializes in extracting information:
            - Extract specific data from text
            - Parse documents (PDF, images)
            - Transcribe audio/video
            - Structure unstructured data

            Use this agent for pulling information from various sources.""",
            "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
        })
        self.orchestrator.add_agent(extraction_agent)

        # Text Transformation Agent
        transformation_agent = BedrockLLMAgent({
            "name": "Text Transformation Agent",
            "description": """Specializes in transforming text:
            - Translation
            - Summarization
            - PII redaction
            - Text generation

            Use this agent for modifying or converting text.""",
            "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
        })
        self.orchestrator.add_agent(transformation_agent)

        # Semantic Operations Agent
        semantic_agent = BedrockLLMAgent({
            "name": "Semantic Operations Agent",
            "description": """Specializes in semantic operations:
            - Generate embeddings
            - Calculate similarity
            - Semantic joins
            - Vector operations

            Use this agent for semantic search and similarity tasks.""",
            "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
        })
        self.orchestrator.add_agent(semantic_agent)

    async def route_request(
        self,
        user_input: str,
        user_id: str,
        session_id: str,
        additional_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Route request to appropriate agent.

        Strands orchestrator automatically selects the best agent based on
        the user input and agent descriptions.
        """
        response = await self.orchestrator.route_request(
            user_input,
            user_id,
            session_id,
            additional_params or {}
        )

        return {
            "agent_name": response.metadata.get("agent_name") if hasattr(response, 'metadata') else None,
            "response": response.output if hasattr(response, 'output') else str(response),
            "metadata": response.metadata if hasattr(response, 'metadata') else {},
        }


# ============================================================================
# Factory Functions
# ============================================================================


def create_aisql_agent(
    snowflake_service: SnowflakeService,
    agent_name: str = "AI SQL Agent",
) -> AISQLStrandsAgent:
    """Create a Strands-integrated AI SQL agent.

    Usage:
        agent = create_aisql_agent(sf_service)
        result = await agent.process_request(
            "Analyze sentiment of reviews",
            "user-123",
            "session-456"
        )
    """
    return AISQLStrandsAgent(snowflake_service, agent_name)


def create_aisql_orchestrator(
    snowflake_service: SnowflakeService,
) -> AISQLOrchestrator:
    """Create a multi-agent orchestrator for AI SQL.

    Usage:
        orchestrator = create_aisql_orchestrator(sf_service)
        result = await orchestrator.route_request(
            "Translate reviews to Spanish",
            "user-123",
            "session-456"
        )
    """
    return AISQLOrchestrator(snowflake_service)


# ============================================================================
# Decorator-based Tool Registration (Alternative Pattern)
# ============================================================================


def register_aisql_tools_as_decorators(agent_class):
    """Decorator pattern for registering AI SQL tools with Strands agents.

    This is an alternative pattern where tools are defined as decorated methods.

    Usage:
        @register_aisql_tools_as_decorators
        class MyAISQLAgent(BedrockLLMAgent):
            def __init__(self, sf_service):
                self.ai_sql = ModularAISQLService(sf_service)
                super().__init__(config)
    """
    from functools import wraps

    # This would typically use Strands' @tool decorator
    # For now, we'll show the pattern

    def tool(name: str, description: str):
        """Tool decorator (mimics Strands pattern)."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            # Register tool metadata
            wrapper._tool_name = name
            wrapper._tool_description = description
            return wrapper
        return decorator

    # Add tool methods to the agent class
    @tool("ai_sentiment", "Analyze sentiment of text")
    async def analyze_sentiment(self, table_name: str, text_column: str):
        """Analyze sentiment using AI SQL."""
        return await self.ai_sql.ai_sentiment(text_column, table_name)

    @tool("ai_classify", "Classify text into categories")
    async def classify_text(
        self,
        table_name: str,
        content_column: str,
        categories: list[str],
    ):
        """Classify text using AI SQL."""
        return await self.ai_sql.ai_classify(content_column, categories, table_name)

    # Attach methods to class
    agent_class.analyze_sentiment = analyze_sentiment
    agent_class.classify_text = classify_text

    return agent_class


# ============================================================================
# Example Usage
# ============================================================================


async def example_usage():
    """Example of using Strands-integrated AI SQL agents."""
    from app.services.snowflake_service import SnowflakeService

    # Initialize Snowflake service
    sf_service = SnowflakeService(connection)

    # Example 1: Single Agent
    agent = create_aisql_agent(sf_service)
    result = await agent.process_request(
        "Analyze sentiment of customer reviews in the reviews table",
        user_id="user-123",
        session_id="session-456",
        additional_params={"table_name": "reviews", "text_column": "review_text"}
    )
    print(f"Tool used: {result['tool_used']}")
    print(f"Results: {result['results']}")

    # Example 2: Multi-Agent Orchestrator
    orchestrator = create_aisql_orchestrator(sf_service)
    result = await orchestrator.route_request(
        "Translate product descriptions from English to Spanish",
        user_id="user-123",
        session_id="session-456",
        additional_params={"table_name": "products"}
    )
    print(f"Agent selected: {result['agent_name']}")
    print(f"Response: {result['response']}")

    # Example 3: Streaming Response
    agent = create_aisql_agent(sf_service)
    response = await agent.agent.process_request(
        "Summarize all customer feedback",
        "user-123",
        "session-456",
        {"streaming": True}
    )

    if response.streaming:
        async for chunk in response.output:
            print(chunk, end="", flush=True)
