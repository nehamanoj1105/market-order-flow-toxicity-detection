from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from app.core.config import settings
import asyncpg
import io
import csv
import json
from typing import List, Optional

from app.core.database import get_db_pool, write_audit_log
from app.core.auth import authenticate_user, create_access_token, get_current_user
from app.api.schemas import (
    AlertOut,
    SymbolConfigSchema,
    ConfigUpdateResponse,
    LoginRequest,
    LoginResponse,
)
from fastapi import WebSocket, WebSocketDisconnect
from app.core.websocket import manager


router = APIRouter()


@router.get("/alerts/history")
async def get_alert_history(
    symbol: Optional[str] = Query(None),
    from_time: Optional[str] = Query(None),
    to_time: Optional[str] = Query(None),
    min_z_score: Optional[float] = Query(None),
    max_z_score: Optional[float] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    conditions = []
    args = []

    if symbol:
        args.append(symbol.strip().upper())
        conditions.append(f"symbol = ${len(args)}")

    if from_time:
        args.append(from_time)
        conditions.append(
            f"alerted_at >= ${len(args)}::timestamptz"
        )

    if to_time:
        args.append(to_time)
        conditions.append(
            f"alerted_at <= ${len(args)}::timestamptz"
        )

    if min_z_score is not None:
        args.append(min_z_score)
        conditions.append(
            f"ABS(z_score) >= ${len(args)}"
        )

    if max_z_score is not None:
        args.append(max_z_score)
        conditions.append(
            f"ABS(z_score) <= ${len(args)}"
        )

    where = ""

    if conditions:
        where = " WHERE " + " AND ".join(conditions)

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM alerts{where}",
            *args,
        )

        rows = await conn.fetch(
            f"""
            SELECT
                id,
                event_id,
                symbol,
                exchange,
                z_score,
                ewma,
                ewma_var,
                signed_ofi,
                side,
                quantity,
                price,
                trade_time_ms,
                alerted_at
            FROM alerts
            {where}
            ORDER BY alerted_at DESC
            LIMIT ${len(args) + 1}
            OFFSET ${len(args) + 2}
            """,
            *args,
            limit,
            offset,
        )

    return {
        "items": [dict(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/alerts", response_model=List[AlertOut])
async def get_alerts(
    symbol: Optional[str] = Query(
        None,
        description="Filter by symbol, e.g. BTCUSDT",
    ),
    min_z_score: Optional[float] = Query(
        None,
        description="Filter by minimum Z-Score",
    ),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """Retrieve historical toxicity alerts with filtering and pagination."""

    query = """
        SELECT
            id,
            event_id,
            symbol,
            exchange,
            z_score,
            ewma,
            ewma_var,
            signed_ofi,
            side,
            quantity,
            price,
            trade_time_ms,
            alerted_at
        FROM alerts
    """

    conditions = []
    args = []

    if symbol:
        args.append(symbol.upper())
        conditions.append(f"symbol = ${len(args)}")

    if min_z_score is not None:
        args.append(min_z_score)
        conditions.append(
            f"ABS(z_score) >= ${len(args)}"
        )

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    args.extend([limit, offset])

    query += (
        f" ORDER BY alerted_at DESC "
        f"LIMIT ${len(args) - 1} "
        f"OFFSET ${len(args)}"
    )

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    return [dict(row) for row in rows]


@router.post("/auth/login", response_model=LoginResponse)
async def login(credentials: LoginRequest):
    """Authenticate the risk administrator."""

    if not authenticate_user(
        credentials.username,
        credentials.password,
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token(credentials.username)

    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": settings.JWT_EXPIRE_MINUTES * 60,
    }


@router.get(
    "/config",
    response_model=List[SymbolConfigSchema],
)
async def get_configurations(
    pool: asyncpg.Pool = Depends(get_db_pool),
    current_user: dict = Depends(get_current_user),
):
    """Fetch current threshold configurations for all tracked symbols."""

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                symbol,
                z_threshold,
                ewma_alpha,
                min_trades
            FROM symbol_configs
            ORDER BY symbol ASC
            """
        )

    return [dict(row) for row in rows]


@router.post(
    "/config",
    response_model=ConfigUpdateResponse,
)
async def update_configuration(
    cfg: SymbolConfigSchema,
    pool: asyncpg.Pool = Depends(get_db_pool),
    current_user: dict = Depends(get_current_user),
):
    """Update scoring configuration and record the change in the audit log."""

    symbol = cfg.symbol.upper()

    query = """
        INSERT INTO symbol_configs (
            symbol,
            z_threshold,
            ewma_alpha,
            min_trades,
            update_time
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            NOW()
        )

        ON CONFLICT (symbol) DO UPDATE SET
            z_threshold = EXCLUDED.z_threshold,
            ewma_alpha = EXCLUDED.ewma_alpha,
            min_trades = EXCLUDED.min_trades,
            update_time = NOW()

        RETURNING
            symbol,
            z_threshold,
            ewma_alpha,
            min_trades;
    """

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            query,
            symbol,
            cfg.z_threshold,
            cfg.ewma_alpha,
            cfg.min_trades,
        )

    updated_config = dict(row)

    await write_audit_log(
        "CONFIG_CHANGE",
        actor=current_user["username"],
        symbol=symbol,
        details={
            "z_threshold": cfg.z_threshold,
            "ewma_alpha": cfg.ewma_alpha,
            "min_trades": cfg.min_trades,
        },
    )

    return {
        "status": "success",
        "symbol": symbol,
        "updated_config": updated_config,
    }


@router.get("/export")
async def export_alerts_csv(
    symbol: Optional[str] = Query(None),
    pool: asyncpg.Pool = Depends(get_db_pool),
    current_user: dict = Depends(get_current_user),
):
    """Export historical toxicity alerts as CSV (UC-05)."""

    query = """
        SELECT
            id,
            event_id,
            symbol,
            exchange,
            z_score,
            ewma,
            signed_ofi,
            side,
            quantity,
            price,
            alerted_at
        FROM alerts
    """

    args = []

    if symbol:
        args.append(symbol.upper())
        query += " WHERE symbol = $1"

    query += " ORDER BY alerted_at DESC LIMIT 5000"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "ID",
            "Event ID",
            "Symbol",
            "Exchange",
            "Z-Score",
            "EWMA",
            "Signed OFI",
            "Side",
            "Quantity",
            "Price",
            "Alerted At",
        ]
    )

    for row in rows:
        writer.writerow(
            [
                row["id"],
                str(row["event_id"]),
                row["symbol"],
                row["exchange"],
                row["z_score"],
                row["ewma"],
                row["signed_ofi"],
                row["side"],
                row["quantity"],
                row["price"],
                row["alerted_at"].isoformat(),
            ]
        )

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": (
                f"attachment; "
                f"filename=toxicity_alerts_"
                f"{symbol or 'all'}.csv"
            )
        },
    )


@router.get("/audit")
async def get_audit_logs(
    limit: int = Query(100, ge=1, le=500),
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    """Retrieve persistent audit logs for UC-04."""

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                event_type,
                actor,
                symbol,
                event_id,
                details,
                created_at,
                previous_hash,
                entry_hash
            FROM audit_logs
            ORDER BY id DESC
            LIMIT $1
            """,
            limit,
        )

    result = []

    for row in rows:
        item = dict(row)

        if isinstance(item["details"], str):
            item["details"] = json.loads(item["details"])

        result.append(item)

    return result


@router.websocket("/ws/alerts")
async def alerts_websocket(websocket: WebSocket):
    await manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(websocket)
