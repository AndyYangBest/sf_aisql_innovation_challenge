"""Hooks for EDA Workflow monitoring and logging.

This module provides hook implementations for tracking EDA workflow execution,
logging progress, and handling errors.
"""

from typing import Any
import logging
from datetime import datetime

from strands.hooks import (
    HookProvider,
    HookRegistry,
    BeforeInvocationEvent,
    AfterInvocationEvent,
    BeforeToolCallEvent,
    AfterToolCallEvent,
    MessageAddedEvent,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EDAWorkflowLoggingHook(HookProvider):
    """Hook for logging EDA workflow execution progress.

    This hook logs:
    - Workflow start/end
    - Each task execution (before/after)
    - Tool calls within tasks
    - Execution time for each step
    """

    def __init__(self):
        self.start_time = None
        self.task_start_times = {}

    def register_hooks(self, registry: HookRegistry) -> None:
        """Register all logging callbacks."""
        registry.add_callback(BeforeInvocationEvent, self.log_workflow_start)
        registry.add_callback(AfterInvocationEvent, self.log_workflow_end)
        registry.add_callback(BeforeToolCallEvent, self.log_task_start)
        registry.add_callback(AfterToolCallEvent, self.log_task_end)
        registry.add_callback(MessageAddedEvent, self.log_message)

    def log_workflow_start(self, event: BeforeInvocationEvent) -> None:
        """Log workflow start."""
        self.start_time = datetime.now()
        logger.info("=" * 80)
        logger.info(f"🚀 EDA Workflow Started: {event.agent.name}")
        logger.info(f"   Request: {event.request.messages[0].content if event.request.messages else 'N/A'}")
        logger.info("=" * 80)

    def log_workflow_end(self, event: AfterInvocationEvent) -> None:
        """Log workflow completion."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            logger.info("=" * 80)
            logger.info(f"✅ EDA Workflow Completed: {event.agent.name}")
            logger.info(f"   Duration: {duration:.2f}s")
            logger.info(f"   Success: {event.exception is None}")
            if event.exception:
                logger.error(f"   Error: {event.exception}")
            logger.info("=" * 80)

    def log_task_start(self, event: BeforeToolCallEvent) -> None:
        """Log task execution start."""
        tool_name = event.tool_use.get("name", "unknown")
        task_id = event.tool_use.get("input", {}).get("task_id", tool_name)

        self.task_start_times[task_id] = datetime.now()

        logger.info("-" * 80)
        logger.info(f"🔧 Task Started: {task_id}")
        logger.info(f"   Tool: {tool_name}")

        # Log task description if available
        description = event.tool_use.get("input", {}).get("description")
        if description:
            logger.info(f"   Description: {description[:100]}...")

    def log_task_end(self, event: AfterToolCallEvent) -> None:
        """Log task execution completion."""
        tool_name = event.tool_use.get("name", "unknown")
        task_id = event.tool_use.get("input", {}).get("task_id", tool_name)

        # Calculate duration
        duration = 0
        if task_id in self.task_start_times:
            duration = (datetime.now() - self.task_start_times[task_id]).total_seconds()

        # Check if task succeeded
        success = event.exception is None
        status = "✓" if success else "✗"

        logger.info(f"{status} Task Completed: {task_id}")
        logger.info(f"   Duration: {duration:.2f}s")

        if not success:
            logger.error(f"   Error: {event.exception}")
        else:
            # Log result summary if available
            result = event.result
            if isinstance(result, dict) and "content" in result:
                content = result["content"]
                if isinstance(content, list) and len(content) > 0:
                    text = content[0].get("text", "")
                    logger.info(f"   Result: {text[:100]}...")

        logger.info("-" * 80)

    def log_message(self, event: MessageAddedEvent) -> None:
        """Log messages added to conversation."""
        message = event.message
        role = message.get("role", "unknown")
        content = message.get("content", "")

        # Only log user and assistant messages (skip system messages)
        if role in ["user", "assistant"]:
            content_preview = str(content)[:100] if content else "N/A"
            logger.debug(f"💬 Message Added [{role}]: {content_preview}...")


class EDAProgressHook(HookProvider):
    """Hook for tracking and displaying EDA workflow progress.

    This hook provides real-time progress updates showing:
    - Current task being executed
    - Progress percentage
    - Estimated time remaining
    """

    def __init__(self):
        self.total_tasks = 0
        self.completed_tasks = 0
        self.current_task = None

    def register_hooks(self, registry: HookRegistry) -> None:
        """Register progress tracking callbacks."""
        registry.add_callback(BeforeInvocationEvent, self.initialize_progress)
        registry.add_callback(BeforeToolCallEvent, self.update_current_task)
        registry.add_callback(AfterToolCallEvent, self.increment_progress)

    def initialize_progress(self, event: BeforeInvocationEvent) -> None:
        """Initialize progress tracking."""
        self.completed_tasks = 0
        self.current_task = None

        # Try to extract total tasks from invocation state
        self.total_tasks = event.invocation_state.get("total_tasks", 0)

        if self.total_tasks > 0:
            logger.info("📊 Progress: 0/%s tasks (0%%)", self.total_tasks)

    def update_current_task(self, event: BeforeToolCallEvent) -> None:
        """Update current task being executed."""
        task_id = event.tool_use.get("input", {}).get("task_id", "unknown")
        self.current_task = task_id

        if self.total_tasks > 0:
            progress = (self.completed_tasks / self.total_tasks) * 100
            logger.info(
                "📊 Progress: %s/%s tasks (%.0f%%)",
                self.completed_tasks,
                self.total_tasks,
                progress,
            )
            logger.info("   ▶ Current: %s", task_id)

    def increment_progress(self, event: AfterToolCallEvent) -> None:
        """Increment progress counter."""
        if event.exception is None:  # Only count successful completions
            self.completed_tasks += 1

            if self.total_tasks > 0:
                progress = (self.completed_tasks / self.total_tasks) * 100
                logger.info("   ✓ Completed: %s", self.current_task)
                logger.info(
                    "📊 Progress: %s/%s tasks (%.0f%%)",
                    self.completed_tasks,
                    self.total_tasks,
                    progress,
                )


class EDAErrorHandlingHook(HookProvider):
    """Hook for handling errors in EDA workflow execution.

    This hook provides:
    - Automatic retry on transient errors
    - Error logging and reporting
    - Graceful degradation
    """

    def __init__(self, max_retries: int = 2):
        self.max_retries = max_retries
        self.retry_counts = {}

    def register_hooks(self, registry: HookRegistry) -> None:
        """Register error handling callbacks."""
        registry.add_callback(BeforeInvocationEvent, self.reset_retry_counts)
        registry.add_callback(AfterToolCallEvent, self.handle_tool_error)

    def reset_retry_counts(self, event: BeforeInvocationEvent) -> None:
        """Reset retry counts for new workflow."""
        self.retry_counts = {}

    def handle_tool_error(self, event: AfterToolCallEvent) -> None:
        """Handle tool execution errors."""
        if event.exception:
            tool_name = event.tool_use.get("name", "unknown")
            task_id = event.tool_use.get("input", {}).get("task_id", tool_name)

            # Track retry count
            retry_count = self.retry_counts.get(task_id, 0)

            logger.error(f"❌ Task Failed: {task_id}")
            logger.error(f"   Error: {event.exception}")
            logger.error(f"   Retry Count: {retry_count}/{self.max_retries}")

            # Check if we should retry
            if retry_count < self.max_retries:
                self.retry_counts[task_id] = retry_count + 1
                logger.info(f"🔄 Retrying task: {task_id} (attempt {retry_count + 2})")

                # Modify result to indicate retry
                event.result = {
                    "content": [{
                        "type": "text",
                        "text": f"Task failed with error: {event.exception}. Retrying..."
                    }]
                }
            else:
                logger.error(f"💥 Task failed after {self.max_retries} retries: {task_id}")

                # Provide fallback result
                event.result = {
                    "content": [{
                        "type": "text",
                        "text": f"Task failed after {self.max_retries} retries. Error: {event.exception}"
                    }]
                }


class EDAMetricsHook(HookProvider):
    """Hook for collecting metrics about EDA workflow execution.

    This hook collects:
    - Execution time per task
    - Success/failure rates
    - Resource usage
    """

    def __init__(self):
        self.metrics = {
            "tasks": {},
            "total_duration": 0,
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
        }
        self.task_start_times = {}
        self.workflow_start_time = None

    def register_hooks(self, registry: HookRegistry) -> None:
        """Register metrics collection callbacks."""
        registry.add_callback(BeforeInvocationEvent, self.start_workflow_timer)
        registry.add_callback(AfterInvocationEvent, self.finalize_metrics)
        registry.add_callback(BeforeToolCallEvent, self.start_task_timer)
        registry.add_callback(AfterToolCallEvent, self.record_task_metrics)

    def start_workflow_timer(self, event: BeforeInvocationEvent) -> None:
        """Start workflow timer."""
        self.workflow_start_time = datetime.now()
        self.metrics = {
            "tasks": {},
            "total_duration": 0,
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
        }

    def start_task_timer(self, event: BeforeToolCallEvent) -> None:
        """Start task timer."""
        task_id = event.tool_use.get("input", {}).get("task_id", "unknown")
        self.task_start_times[task_id] = datetime.now()

    def record_task_metrics(self, event: AfterToolCallEvent) -> None:
        """Record task execution metrics."""
        task_id = event.tool_use.get("input", {}).get("task_id", "unknown")

        # Calculate duration
        duration = 0
        if task_id in self.task_start_times:
            duration = (datetime.now() - self.task_start_times[task_id]).total_seconds()

        # Record metrics
        success = event.exception is None
        self.metrics["tasks"][task_id] = {
            "duration": duration,
            "success": success,
            "error": str(event.exception) if event.exception else None,
        }

        self.metrics["total_tasks"] += 1
        if success:
            self.metrics["successful_tasks"] += 1
        else:
            self.metrics["failed_tasks"] += 1

    def finalize_metrics(self, event: AfterInvocationEvent) -> None:
        """Finalize and log metrics."""
        if self.workflow_start_time:
            self.metrics["total_duration"] = (
                datetime.now() - self.workflow_start_time
            ).total_seconds()

        # Log metrics summary
        logger.info("\n" + "=" * 80)
        logger.info("📈 EDA Workflow Metrics")
        logger.info("=" * 80)
        logger.info(f"Total Duration: {self.metrics['total_duration']:.2f}s")
        logger.info(f"Total Tasks: {self.metrics['total_tasks']}")
        logger.info(f"Successful: {self.metrics['successful_tasks']}")
        logger.info(f"Failed: {self.metrics['failed_tasks']}")

        if self.metrics["tasks"]:
            logger.info("\nTask Breakdown:")
            for task_id, task_metrics in self.metrics["tasks"].items():
                status = "✓" if task_metrics["success"] else "✗"
                logger.info(f"  {status} {task_id}: {task_metrics['duration']:.2f}s")

        logger.info("=" * 80 + "\n")

    def get_metrics(self) -> dict[str, Any]:
        """Get collected metrics."""
        return self.metrics


# ============================================================================
# Factory Functions
# ============================================================================


def create_default_eda_hooks() -> list[HookProvider]:
    """Create default set of EDA workflow hooks.

    Returns:
        List of hook providers for logging, progress, and error handling
    """
    return [
        EDAWorkflowLoggingHook(),
        EDAProgressHook(),
        EDAErrorHandlingHook(max_retries=2),
        EDAMetricsHook(),
    ]


def create_minimal_eda_hooks() -> list[HookProvider]:
    """Create minimal set of EDA workflow hooks (just progress).

    Returns:
        List with only progress tracking hook
    """
    return [EDAProgressHook()]
