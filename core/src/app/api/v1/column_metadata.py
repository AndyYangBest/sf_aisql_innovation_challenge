"""API endpoints for column metadata caching and initialization."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from ...api.dependencies import get_ai_sql_service, get_snowflake_service
from ...core.db.database import get_async_db_session
from ...models.report_layout_event import ReportLayoutEvent
from ...schemas.column_metadata import (
    ColumnMetadataList,
    ColumnMetadataOverrideRequest,
    ColumnMetadataRead,
    TableAssetMetadataOverrideRequest,
    TableAssetMetadataRead,
)
from ...services.column_metadata_service import ColumnMetadataService
from ...services.modular_ai_sql_service import ModularAISQLService
from ...services.snowflake_service import SnowflakeService

router = APIRouter(prefix="/column-metadata", tags=["Column Metadata"])


def _coerce_layout_int(value: Any, minimum: int) -> int | None:
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return None


def _parse_card_identity(card_id: str) -> tuple[str, str]:
    prefix, sep, suffix = card_id.partition(":")
    if not sep:
        return "unknown", card_id
    kind = prefix if prefix in {"chart", "insight"} else "unknown"
    artifact_id = suffix or card_id
    return kind, artifact_id


def _extract_report_layout(overrides: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(overrides, dict):
        return {}

    report_payload = overrides.get("report")
    if not isinstance(report_payload, dict):
        return {}

    raw_layout = report_payload.get("layout")
    if not isinstance(raw_layout, dict):
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    for card_id, raw_item in raw_layout.items():
        if not isinstance(card_id, str) or not isinstance(raw_item, dict):
            continue

        x = _coerce_layout_int(raw_item.get("x"), minimum=0)
        y = _coerce_layout_int(raw_item.get("y"), minimum=0)
        w = _coerce_layout_int(raw_item.get("w"), minimum=1)
        h = _coerce_layout_int(raw_item.get("h"), minimum=1)
        if x is None or y is None or w is None or h is None:
            continue

        parsed_kind, parsed_artifact_id = _parse_card_identity(card_id)
        card_kind = raw_item.get("kind")
        if isinstance(card_kind, str) and card_kind in {"chart", "insight"}:
            normalized_kind = card_kind
        else:
            normalized_kind = parsed_kind

        raw_artifact_id = raw_item.get("artifactId")
        if not isinstance(raw_artifact_id, str) or not raw_artifact_id:
            raw_artifact_id = raw_item.get("artifact_id")
        artifact_id = raw_artifact_id if isinstance(raw_artifact_id, str) and raw_artifact_id else parsed_artifact_id

        normalized[card_id] = {
            "x": x,
            "y": y,
            "w": w,
            "h": h,
            "kind": normalized_kind,
            "artifactId": artifact_id,
        }

    return normalized


def _infer_layout_event_type(previous: dict[str, Any] | None, current: dict[str, Any]) -> str:
    if previous is None:
        return "add"
    moved = previous.get("x") != current.get("x") or previous.get("y") != current.get("y")
    resized = previous.get("w") != current.get("w") or previous.get("h") != current.get("h")
    if moved and resized:
        return "move_resize"
    if resized:
        return "resize"
    if moved:
        return "move"
    return "noop"


def _merge_table_overrides(existing: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing or {})
    payload = dict(incoming or {})

    incoming_report = payload.pop("report", None)
    if isinstance(incoming_report, dict):
        report_overrides = dict(merged.get("report") or {})
        report_overrides.update(incoming_report)
        merged["report"] = report_overrides

    merged.update(payload)
    return merged


def _make_layout_event(
    *,
    table_asset_id: int,
    card_id: str,
    artifact_id: str,
    card_kind: str,
    event_type: str,
    x: int,
    y: int,
    w: int,
    h: int,
) -> ReportLayoutEvent:
    event = ReportLayoutEvent()
    event.table_asset_id = table_asset_id
    event.card_id = card_id
    event.artifact_id = artifact_id
    event.card_kind = card_kind
    event.event_type = event_type
    event.x = x
    event.y = y
    event.w = w
    event.h = h
    return event


@router.get("/{table_asset_id}", response_model=ColumnMetadataList)
async def get_column_metadata(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Fetch cached column metadata for a table asset."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta) if table_meta else None,
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.post("/{table_asset_id}/initialize", response_model=ColumnMetadataList)
async def initialize_column_metadata(
    table_asset_id: int,
    force: bool = Query(False, description="Force refresh even if cache exists"),
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Initialize column metadata with sampling and inference."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    try:
        table_meta, columns = await service.initialize_metadata(table_asset_id, force=force)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta),
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.put("/{table_asset_id}/override", response_model=ColumnMetadataList)
async def override_column_metadata(
    table_asset_id: int,
    request: ColumnMetadataOverrideRequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Override column metadata based on user input."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)
    if not columns:
        raise HTTPException(status_code=404, detail="Column metadata not found")

    target = next((col for col in columns if col.column_name == request.column_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="Column not found")

    overrides = dict(target.overrides or {})
    overrides.update(request.overrides)
    target.overrides = overrides
    flag_modified(target, "overrides")

    await db.commit()
    await db.refresh(target)

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta) if table_meta else None,
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.put("/{table_asset_id}/bulk-override", response_model=ColumnMetadataList)
async def bulk_override_column_metadata(
    table_asset_id: int,
    requests: list[ColumnMetadataOverrideRequest],
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Override column metadata in bulk based on user input."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)
    if not columns:
        try:
            table_meta, columns = await service.initialize_metadata(table_asset_id, force=False)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    column_map = {col.column_name: col for col in columns}
    for request in requests:
        target = column_map.get(request.column_name)
        if not target:
            continue
        overrides = dict(target.overrides or {})
        overrides.update(request.overrides)
        target.overrides = overrides
        flag_modified(target, "overrides")

    await db.commit()

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta) if table_meta else None,
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.put("/{table_asset_id}/table-override", response_model=ColumnMetadataList)
async def override_table_metadata(
    table_asset_id: int,
    request: TableAssetMetadataOverrideRequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Override table metadata based on user input."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)
    if not table_meta:
        try:
            table_meta, columns = await service.initialize_metadata(table_asset_id, force=False)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    existing_overrides = dict(table_meta.overrides or {})
    existing_layout = _extract_report_layout(existing_overrides)
    overrides = _merge_table_overrides(existing_overrides, request.overrides)
    next_layout = _extract_report_layout(overrides)

    for card_id, current in next_layout.items():
        previous = existing_layout.get(card_id)
        event_type = _infer_layout_event_type(previous, current)
        if event_type == "noop":
            continue
        db.add(
            _make_layout_event(
                table_asset_id=table_asset_id,
                card_id=card_id,
                artifact_id=current["artifactId"],
                card_kind=current["kind"],
                event_type=event_type,
                x=current["x"],
                y=current["y"],
                w=current["w"],
                h=current["h"],
            )
        )

    for card_id, previous in existing_layout.items():
        if card_id in next_layout:
            continue
        db.add(
            _make_layout_event(
                table_asset_id=table_asset_id,
                card_id=card_id,
                artifact_id=previous["artifactId"],
                card_kind=previous["kind"],
                event_type="remove",
                x=previous["x"],
                y=previous["y"],
                w=previous["w"],
                h=previous["h"],
            )
        )

    report_payload = overrides.get("report")
    if isinstance(report_payload, dict):
        normalized_report = dict(report_payload)
        normalized_report["layout"] = next_layout
        overrides["report"] = normalized_report

    table_meta.overrides = overrides
    flag_modified(table_meta, "overrides")

    await db.commit()
    await db.refresh(table_meta)

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta),
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )
