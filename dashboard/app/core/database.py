import asyncpg
from typing import Optional
import logging
import json

logger = logging.getLogger("dashboard.db")

class Database:
    pool: Optional[asyncpg.Pool] = None

db = Database()

async def connect_db(dsn: str):
    logger.info("Initializing Postgres/TimescaleDB connection pool...")
    db.pool = await asyncpg.create_pool(dsn=dsn, min_size=5, max_size=20, command_timeout=60)
    logger.info("Database connection pool created.")

async def disconnect_db():
    if db.pool:
        logger.info("Closing database pool...")
        await db.pool.close()
        logger.info("Database pool closed.")

async def get_db_pool() -> asyncpg.Pool:
    return db.pool

async def write_audit_log(
    event_type: str,
    actor: str = "system",
    symbol: Optional[str] = None,
    event_id: Optional[str] = None,
    details: Optional[dict] = None,
):
    """
    Store an audit event.

    Hash chaining (previous_hash / entry_hash) is handled entirely by
    the PostgreSQL trigger ``trg_audit_log_hash_chain`` defined in
    ``infra/init.sql``.  We only need to insert the payload fields.
    """

    if db.pool is None:
        raise RuntimeError("Database pool is not initialized")

    details = details or {}

    details_json = json.dumps(
        details,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    await db.pool.execute(
        """
        INSERT INTO audit_logs (
            event_type,
            actor,
            symbol,
            event_id,
            details
        )
        VALUES ($1, $2, $3, $4::uuid, $5::jsonb)
        """,
        event_type,
        actor,
        symbol,
        event_id,
        details_json,
    )
