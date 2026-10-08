import asyncpg
from typing import Optional
import logging
import json
import hashlib

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
    Store an audit event with a chained SHA-256 hash.
    """

    if db.pool is None:
        raise RuntimeError("Database pool is not initialized")

    details = details or {}

    async with db.pool.acquire() as conn:
        previous_hash = await conn.fetchval(
            """
            SELECT entry_hash
            FROM audit_logs
            ORDER BY id DESC
            LIMIT 1
            """
        )

        details_json = json.dumps(
            details,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

        hash_input = "|".join([
            event_type,
            actor,
            symbol or "",
            event_id or "",
            details_json,
            previous_hash or "",
        ])

        entry_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

        await conn.execute(
            """
            INSERT INTO audit_logs (
                event_type,
                actor,
                symbol,
                event_id,
                details,
                previous_hash,
                entry_hash
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
            """,
            event_type,
            actor,
            symbol,
            event_id,
            details_json,
            previous_hash,
            entry_hash,
        )
