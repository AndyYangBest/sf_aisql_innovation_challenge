"""Workflow logging utilities for Strands column workflows."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from strands.hooks import (
    AfterInvocationEvent,
    AfterToolCallEvent,
    BeforeInvocationEvent,
    BeforeToolCallEvent,
    HookProvider,
    HookRegistry,
    MessageAddedEvent,
)


class ColumnWorkflowLogBuffer:
    """Capture Strands agent logs and tool calls for UI inspection."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []
        self.tool_calls: list[dict[str, Any]] = []
        self._sequence = 0
        self._running_batches: dict[str, dict[str, Any]] = {}
        self.default_table_asset_id: int | None = None
        self.default_column_name: str | None = None
        self._last_synced_entries = 0
        self._last_synced_tool_calls = 0
        self.sync_failed = False
        self.sync_error: str | None = None

    def set_default_context(self, table_asset_id: int, column_name: str) -> None:
        self.default_table_asset_id = table_asset_id
        self.default_column_name = column_name

    def has_updates(self) -> bool:
        return (
            len(self.entries) > self._last_synced_entries
            or len(self.tool_calls) > self._last_synced_tool_calls
        )

    def mark_synced(self) -> None:
        self._last_synced_entries = len(self.entries)
        self._last_synced_tool_calls = len(self.tool_calls)

    def add_entry(self, event_type: str, message: str, data: dict[str, Any] | None = None) -> None:
        entry = {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "message": message,
            "data": data or {},
        }
        self.entries.append(entry)

    def add_tool_call(
        self,
        tool_use_id: str | None,
        tool_name: str,
        agent_name: str | None,
        tool_input: dict[str, Any] | None,
    ) -> None:
        self._sequence += 1
        started_at = datetime.utcnow().isoformat()
        if not tool_use_id:
            tool_use_id = f"tool_{self._sequence}"
        agent_key = agent_name or "unknown"
        batch = self._running_batches.get(agent_key)
        if not batch or not batch.get("tool_use_ids"):
            batch = {
                "batch_id": f"batch_{agent_key}_{self._sequence}",
                "tool_use_ids": set(),
            }
            self._running_batches[agent_key] = batch
        batch["tool_use_ids"].add(tool_use_id)
        self.tool_calls.append(
            {
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "agent_name": agent_name,
                "batch_id": batch["batch_id"],
                "input": tool_input or {},
                "status": "running",
                "timestamp": started_at,
                "started_at": started_at,
                "sequence": self._sequence,
            }
        )

    def update_tool_call(
        self,
        tool_use_id: str | None,
        status: str,
        error: str | None = None,
    ) -> None:
        if tool_use_id:
            for call in reversed(self.tool_calls):
                if call.get("tool_use_id") == tool_use_id:
                    call["status"] = status
                    call["ended_at"] = datetime.utcnow().isoformat()
                    started_at = call.get("started_at")
                    if started_at:
                        try:
                            start_dt = datetime.fromisoformat(str(started_at))
                            end_dt = datetime.fromisoformat(str(call["ended_at"]))
                            call["duration_ms"] = int(
                                (end_dt - start_dt).total_seconds() * 1000
                            )
                        except ValueError:
                            pass
                    if error:
                        call["error"] = error
                    agent_key = call.get("agent_name") or "unknown"
                    batch = self._running_batches.get(agent_key)
                    if batch and tool_use_id:
                        batch["tool_use_ids"].discard(tool_use_id)
                        if not batch["tool_use_ids"]:
                            self._running_batches.pop(agent_key, None)
                    return
            return
        for call in reversed(self.tool_calls):
            if call.get("status") == "running":
                call["status"] = status
                call["ended_at"] = datetime.utcnow().isoformat()
                if error:
                    call["error"] = error
                agent_key = call.get("agent_name") or "unknown"
                fallback_id = call.get("tool_use_id")
                batch = self._running_batches.get(agent_key)
                if batch and fallback_id:
                    batch["tool_use_ids"].discard(fallback_id)
                    if not batch["tool_use_ids"]:
                        self._running_batches.pop(agent_key, None)
                return


class ColumnWorkflowLogHook(HookProvider):
    """Hook for capturing Strands agent logs and tool invocations."""

    def __init__(self, buffer: ColumnWorkflowLogBuffer, max_preview: int = 240) -> None:
        self.buffer = buffer
        self.max_preview = max_preview

    def register_hooks(self, registry: HookRegistry) -> None:
        registry.add_callback(BeforeInvocationEvent, self.log_agent_start)
        registry.add_callback(AfterInvocationEvent, self.log_agent_end)
        registry.add_callback(BeforeToolCallEvent, self.log_tool_start)
        registry.add_callback(AfterToolCallEvent, self.log_tool_end)
        registry.add_callback(MessageAddedEvent, self.log_message)

    def _truncate(self, value: Any) -> str:
        raw = str(value) if value is not None else ""
        if len(raw) <= self.max_preview:
            return raw
        return raw[: self.max_preview].rstrip() + "..."

    def _format_content(self, content: Any) -> str:
        if content is None:
            return ""
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    if "text" in item:
                        parts.append(str(item.get("text", "")))
                    elif "json" in item:
                        parts.append(json.dumps(item.get("json"), default=str))
                    else:
                        parts.append(json.dumps(item, default=str))
                else:
                    parts.append(str(item))
            return " ".join(part for part in parts if part)
        if isinstance(content, dict):
            return json.dumps(content, default=str)
        return str(content)

    def _apply_context_overrides(self, event: BeforeToolCallEvent) -> None:
        tool_input = event.tool_use.get("input")
        if not isinstance(tool_input, dict):
            tool_input = {}
        invocation_state = event.invocation_state or {}
        table_asset_id = invocation_state.get("table_asset_id")
        column_name = invocation_state.get("column_name")
        if table_asset_id is None:
            table_asset_id = self.buffer.default_table_asset_id
        if not column_name:
            column_name = self.buffer.default_column_name

        tool_spec = None
        if event.selected_tool:
            tool_spec = getattr(event.selected_tool, "tool_spec", None) or getattr(
                event.selected_tool, "spec", None
            )
        schema = tool_spec.get("inputSchema", {}).get("json", {}) if isinstance(tool_spec, dict) else {}
        properties = schema.get("properties") if isinstance(schema, dict) else {}
        allow_table_id = "table_asset_id" in tool_input or "table_asset_id" in properties
        allow_column = "column_name" in tool_input or "column_name" in properties

        if allow_table_id and table_asset_id is not None:
            tool_input["table_asset_id"] = table_asset_id
        if allow_column and column_name:
            tool_input["column_name"] = column_name
        event.tool_use["input"] = tool_input

    def log_agent_start(self, event: BeforeInvocationEvent) -> None:
        self.buffer.add_entry(
            "strands_log",
            f"Agent started: {event.agent.name}",
            {"agent": event.agent.name},
        )

    def log_agent_end(self, event: AfterInvocationEvent) -> None:
        result = getattr(event, "result", None)
        status = "success" if result is not None else "unknown"
        message = f"Agent completed: {event.agent.name}"
        if result is not None and getattr(result, "stop_reason", None):
            message += f" (stop_reason: {result.stop_reason})"
        self.buffer.add_entry(
            "strands_log",
            message,
            {"agent": event.agent.name, "status": status},
        )

    def log_tool_start(self, event: BeforeToolCallEvent) -> None:
        self._apply_context_overrides(event)
        tool_name = event.tool_use.get("name", "unknown")
        tool_use_id = event.tool_use.get("toolUseId") or event.tool_use.get("tool_use_id")
        if not tool_use_id:
            tool_use_id = f"tool_{len(self.buffer.tool_calls) + 1}"
            event.tool_use["toolUseId"] = tool_use_id
        tool_input = event.tool_use.get("input", {})
        self.buffer.add_tool_call(tool_use_id, tool_name, event.agent.name, tool_input)
        self.buffer.add_entry(
            "status",
            f"Tool started: {tool_name}",
            {"tool_name": tool_name, "state": "running", "input": tool_input},
        )

    def log_tool_end(self, event: AfterToolCallEvent) -> None:
        tool_name = event.tool_use.get("name", "unknown")
        tool_use_id = event.tool_use.get("toolUseId") or event.tool_use.get("tool_use_id")
        exception = getattr(event, "exception", None)
        status = "error" if exception else "success"
        error = str(exception) if exception else None
        self.buffer.update_tool_call(tool_use_id, status, error=error)
        message = f"Tool completed: {tool_name}"
        if error:
            message += f" (error: {error})"
        self.buffer.add_entry(
            "status",
            message,
            {"tool_name": tool_name, "state": status},
        )
        if event.result is not None:
            result_preview = self._truncate(self._format_content(event.result))
            if result_preview:
                self.buffer.add_entry(
                    "strands_log",
                    f"Tool result: {tool_name} -> {result_preview}",
                    {"tool_name": tool_name, "state": status},
                )

    def log_message(self, event: MessageAddedEvent) -> None:
        message = event.message
        role = message.get("role", "unknown")
        content = message.get("content", "")
        preview = self._truncate(self._format_content(content))
        self.buffer.add_entry(
            "strands_log",
            f"{role}: {preview}",
            {"role": role},
        )
