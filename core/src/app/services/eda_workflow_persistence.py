"""Service for persisting EDA workflow executions to PostgreSQL."""

import json
import re
from datetime import datetime, timezone
from typing import Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.eda_workflow import EDAWorkflowExecution, EDAWorkflowLog


class EDAWorkflowPersistenceService:
    """Service for saving and retrieving EDA workflow execution data."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_execution(
        self,
        workflow_id: str,
        workflow_type: str,
        table_asset_id: int,
        user_intent: Optional[str] = None,
        user_id: Optional[int] = None,
        tasks_total: int = 0,
    ) -> EDAWorkflowExecution:
        """Create a new workflow execution record.

        Args:
            workflow_id: Unique workflow identifier
            workflow_type: Type of workflow (EDA_OVERVIEW, etc.)
            table_asset_id: ID of the table being analyzed
            user_intent: User's analysis goal
            user_id: ID of user who triggered the workflow
            tasks_total: Total number of tasks in workflow

        Returns:
            Created execution record
        """
        execution = EDAWorkflowExecution(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            table_asset_id=table_asset_id,
            user_intent=user_intent,
            user_id=user_id,
            status="running",
            tasks_total=tasks_total,
            progress=0,
        )

        self.db.add(execution)
        await self.db.commit()
        await self.db.refresh(execution)

        return execution

    async def update_execution(
        self,
        workflow_id: str,
        status: Optional[str] = None,
        progress: Optional[int] = None,
        tasks_completed: Optional[int] = None,
        tasks_failed: Optional[int] = None,
        artifacts: Optional[dict] = None,
        summary: Optional[dict] = None,
        error_message: Optional[str] = None,
    ) -> Optional[EDAWorkflowExecution]:
        """Update an existing workflow execution.

        Args:
            workflow_id: Workflow identifier
            status: New status (completed, failed, etc.)
            progress: Progress percentage (0-100)
            tasks_completed: Number of completed tasks
            tasks_failed: Number of failed tasks
            artifacts: Task results
            summary: Summary information
            error_message: Error details if failed

        Returns:
            Updated execution record or None if not found
        """
        result = await self.db.execute(
            select(EDAWorkflowExecution).where(
                EDAWorkflowExecution.workflow_id == workflow_id
            )
        )
        execution = result.scalar_one_or_none()

        if not execution:
            return None

        # Update fields
        if status is not None:
            execution.status = status
            if status == "completed":
                started_at = execution.started_at
                if started_at and started_at.tzinfo is not None:
                    now = datetime.now(timezone.utc)
                else:
                    now = datetime.utcnow()
                execution.completed_at = now
                if started_at:
                    if started_at.tzinfo is None and now.tzinfo is not None:
                        started_at = started_at.replace(tzinfo=now.tzinfo)
                    execution.duration_seconds = (now - started_at).total_seconds()

        if progress is not None:
            execution.progress = progress

        if tasks_completed is not None:
            execution.tasks_completed = tasks_completed

        if tasks_failed is not None:
            execution.tasks_failed = tasks_failed

        if artifacts is not None:
            execution.artifacts = artifacts

            # Extract type detection results from profile artifact
            if "profile_table" in artifacts:
                type_info = self._extract_type_info_from_artifact(
                    artifacts["profile_table"]
                )
                if type_info:
                    execution.data_structure_type = type_info.get("data_structure_type")
                    execution.column_type_inferences = type_info.get(
                        "column_type_inferences"
                    )

        if summary is not None:
            execution.summary = summary

        if error_message is not None:
            execution.error_message = error_message

        await self.db.commit()
        await self.db.refresh(execution)

        return execution

    async def complete_execution(
        self,
        workflow_id: str,
        artifacts: dict,
        summary: dict,
    ) -> Optional[EDAWorkflowExecution]:
        """Mark a workflow execution as completed.

        Args:
            workflow_id: Workflow identifier
            artifacts: All task results
            summary: Summary information

        Returns:
            Updated execution record or None if not found
        """
        return await self.update_execution(
            workflow_id=workflow_id,
            status="completed",
            progress=100,
            tasks_completed=summary.get("tasks_completed", 0),
            artifacts=artifacts,
            summary=summary,
        )

    async def fail_execution(
        self,
        workflow_id: str,
        error_message: str,
    ) -> Optional[EDAWorkflowExecution]:
        """Mark a workflow execution as failed.

        Args:
            workflow_id: Workflow identifier
            error_message: Error details

        Returns:
            Updated execution record or None if not found
        """
        return await self.update_execution(
            workflow_id=workflow_id,
            status="failed",
            error_message=error_message,
        )

    async def get_execution(
        self, workflow_id: str
    ) -> Optional[EDAWorkflowExecution]:
        """Get a workflow execution by ID.

        Args:
            workflow_id: Workflow identifier

        Returns:
            Execution record or None if not found
        """
        result = await self.db.execute(
            select(EDAWorkflowExecution).where(
                EDAWorkflowExecution.workflow_id == workflow_id
            )
        )
        return result.scalar_one_or_none()

    async def get_executions_for_table(
        self, table_asset_id: int, limit: int = 10
    ) -> list[EDAWorkflowExecution]:
        """Get recent workflow executions for a table.

        Args:
            table_asset_id: Table asset ID
            limit: Maximum number of records to return

        Returns:
            List of execution records
        """
        result = await self.db.execute(
            select(EDAWorkflowExecution)
            .where(
                EDAWorkflowExecution.table_asset_id == table_asset_id,
                EDAWorkflowExecution.is_deleted == False,
            )
            .order_by(EDAWorkflowExecution.created_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def log_event(
        self,
        workflow_execution_id: int,
        log_level: str,
        log_type: str,
        message: str,
        task_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        details: Optional[dict] = None,
        duration_seconds: Optional[float] = None,
    ) -> EDAWorkflowLog:
        """Log an important workflow event.

        Args:
            workflow_execution_id: Execution record ID
            log_level: Log level (INFO, WARNING, ERROR)
            log_type: Event type (workflow_started, task_completed, etc.)
            message: Log message
            task_id: Task identifier (if applicable)
            tool_name: Tool name (if applicable)
            details: Additional structured information
            duration_seconds: Duration (if applicable)

        Returns:
            Created log record
        """
        log = EDAWorkflowLog(
            workflow_execution_id=workflow_execution_id,
            log_level=log_level,
            log_type=log_type,
            message=message,
            task_id=task_id,
            tool_name=tool_name,
            details=details,
            duration_seconds=duration_seconds,
        )

        self.db.add(log)
        await self.db.commit()
        await self.db.refresh(log)

        return log

    def _extract_type_info_from_artifact(
        self, profile_artifact: dict
    ) -> Optional[dict]:
        """Extract type detection info from profile artifact.

        Args:
            profile_artifact: Profile task artifact

        Returns:
            Dictionary with data_structure_type and column_type_inferences
        """
        try:
            # The artifact contains text with JSON
            text_content = profile_artifact.get("text", "")

            # Extract JSON from markdown code block
            json_match = re.search(
                r"```json\s*(\{.*?\})\s*```", text_content, re.DOTALL
            )
            if not json_match:
                return None

            profile_json = json.loads(json_match.group(1))
            metadata = profile_json.get("metadata", {})

            return {
                "data_structure_type": metadata.get("data_structure_type"),
                "column_type_inferences": metadata.get("column_type_inferences", []),
            }

        except Exception:
            return None
