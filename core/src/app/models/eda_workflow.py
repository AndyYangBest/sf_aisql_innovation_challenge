"""EDA Workflow execution tracking models."""

from sqlalchemy import Integer, String, Text, Float, ForeignKey, Boolean, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional

from ..core.db.database import Base


class EDAWorkflowExecution(Base):
    """EDA Workflow execution record.

    Stores the complete execution history of EDA workflows including:
    - Workflow metadata (type, status, timing)
    - Task execution results (artifacts)
    - Type detection results (data structure, column types)
    - Performance metrics
    """

    __tablename__ = "eda_workflow_executions"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, init=False)

    # Workflow identification
    workflow_id: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    workflow_type: Mapped[str] = mapped_column(String(50), index=True, nullable=False)

    # Relationships
    table_asset_id: Mapped[int] = mapped_column(Integer, ForeignKey("table_assets.id"), index=True, nullable=False)
    user_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, default=None)

    # Execution status
    status: Mapped[str] = mapped_column(String(50), index=True, default="pending")
    progress: Mapped[int] = mapped_column(Integer, default=0)

    # Task statistics
    tasks_total: Mapped[int] = mapped_column(Integer, default=0)
    tasks_completed: Mapped[int] = mapped_column(Integer, default=0)
    tasks_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Timing
    started_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False, init=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, default=None)

    # Results (JSON storage)
    artifacts: Mapped[Optional[dict]] = mapped_column(JSON, default=None)
    summary: Mapped[Optional[dict]] = mapped_column(JSON, default=None)

    # Type detection results (extracted for easy querying)
    data_structure_type: Mapped[Optional[str]] = mapped_column(String(50), index=True, default=None)
    column_type_inferences: Mapped[Optional[dict]] = mapped_column(JSON, default=None)

    # Metadata
    user_intent: Mapped[Optional[str]] = mapped_column(Text, default=None)
    error_message: Mapped[Optional[str]] = mapped_column(Text, default=None)

    # Audit fields
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False, init=False)
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now(), nullable=False, init=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)


class EDAWorkflowLog(Base):
    """EDA Workflow execution logs.

    Stores important events during workflow execution for:
    - Debugging and troubleshooting
    - Performance analysis
    - Audit trail

    Note: Only important logs are stored to avoid excessive data growth.
    """

    __tablename__ = "eda_workflow_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, init=False)

    # Foreign key (required)
    workflow_execution_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("eda_workflow_executions.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    # Log classification (required)
    log_level: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    log_type: Mapped[str] = mapped_column(String(50), index=True, nullable=False)

    # Content (required)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    # Context (optional)
    task_id: Mapped[Optional[str]] = mapped_column(String(100), index=True, default=None)
    tool_name: Mapped[Optional[str]] = mapped_column(String(100), index=True, default=None)

    # Additional content (optional)
    details: Mapped[Optional[dict]] = mapped_column(JSON, default=None)

    # Performance (optional)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, default=None)

    # Timestamp
    timestamp: Mapped[datetime] = mapped_column(server_default=func.now(), index=True, nullable=False, init=False)
