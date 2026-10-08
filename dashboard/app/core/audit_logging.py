"""Small asyncpg helper for persistent UC-04 audit logging."""

from typing import Any, Optional

from app.core.database import db


async def write_audit_log(
    event_type: str,
    *,
    actor: str = "system",
    symbol: Optional[str] = None,
    event_id: Optional[str] = None,
    details: Optional[dict[str, Any]] = None,
) -> None:
    if db.pool is None:
        raise RuntimeError("Database pool is not initialized")

    await db.pool.execute(
        """
        INSERT INTO audit_logs (
            event_type, actor, symbol, event_id, details
        )
        VALUES ($1, $2, $3, $4::uuid, $5::jsonb)
        """,
        event_type,
        actor,
        symbol,
        event_id,
        __import__("json").dumps(details or {}),
    )
