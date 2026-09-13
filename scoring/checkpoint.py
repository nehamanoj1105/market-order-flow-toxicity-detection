"""
checkpoint.py – PostgreSQL-backed scorer state persistence.

The schema is deliberately minimal: one row per symbol, updated in-place on
every checkpoint.  The checkpointing contract is:

  1. At startup, call restore_all() to rebuild the in-memory registry from
     the last persisted state.
  2. Periodically (every N trades) call save(state) to persist the current
     state.
  3. On clean shutdown, call save_all(registry) to flush everything.

This means a restart picks up where the scorer left off rather than
resetting to zero (requirement 4).

The table is created lazily on first use so the service can start even if
PostgreSQL is temporarily unavailable (useful for local dev).
"""
from __future__ import annotations

import logging
from typing import List, Optional

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:  # pragma: no cover – optional dependency
    _PSYCOPG2_AVAILABLE = False

from .scorer import ScorerState

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scorer_checkpoints (
    symbol      TEXT PRIMARY KEY,
    ewma        DOUBLE PRECISION NOT NULL,
    ewma_var    DOUBLE PRECISION NOT NULL,
    trade_count BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_UPSERT_SQL = """
INSERT INTO scorer_checkpoints (symbol, ewma, ewma_var, trade_count, updated_at)
VALUES (%(symbol)s, %(ewma)s, %(ewma_var)s, %(trade_count)s, NOW())
ON CONFLICT (symbol)
DO UPDATE SET
    ewma        = EXCLUDED.ewma,
    ewma_var    = EXCLUDED.ewma_var,
    trade_count = EXCLUDED.trade_count,
    updated_at  = EXCLUDED.updated_at;
"""

_SELECT_ALL_SQL = """
SELECT symbol, ewma, ewma_var, trade_count
FROM scorer_checkpoints;
"""


class CheckpointStore:
    """
    PostgreSQL-backed scorer state store.

    Parameters
    ----------
    dsn : str
        PostgreSQL connection string, e.g.
        ``"postgresql://user:pass@host:5432/dbname"``.
    """

    def __init__(self, dsn: str) -> None:
        if not _PSYCOPG2_AVAILABLE:
            raise RuntimeError(
                "psycopg2 is not installed.  "
                "Add 'psycopg2-binary' to requirements.txt."
            )
        self._dsn = dsn
        self._conn: Optional[object] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_conn(self):
        """Return (or re-open) the database connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = False
        return self._conn

    def ensure_schema(self) -> None:
        """Create the checkpoints table if it doesn't exist."""
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
        logger.info("scorer_checkpoints table ready")

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def save(self, state: ScorerState) -> None:
        """
        Persist (upsert) one scorer's state.

        Safe to call frequently – uses INSERT … ON CONFLICT UPDATE so it
        never fails on duplicate keys.
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _UPSERT_SQL,
                    {
                        "symbol": state.symbol,
                        "ewma": state.ewma,
                        "ewma_var": state.ewma_var,
                        "trade_count": state.trade_count,
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def save_all(self, states: List[ScorerState]) -> None:
        """Persist multiple scorer states in one transaction."""
        if not states:
            return
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                for state in states:
                    cur.execute(
                        _UPSERT_SQL,
                        {
                            "symbol": state.symbol,
                            "ewma": state.ewma,
                            "ewma_var": state.ewma_var,
                            "trade_count": state.trade_count,
                        },
                    )
            conn.commit()
            logger.info("checkpointed %d scorer(s)", len(states))
        except Exception:
            conn.rollback()
            raise

    def restore_all(self) -> List[ScorerState]:
        """
        Load all checkpointed states from the database.

        Returns an empty list if the table is empty or doesn't exist.
        """
        try:
            conn = self._get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SELECT_ALL_SQL)
                rows = cur.fetchall()
            states = [ScorerState.from_dict(dict(row)) for row in rows]
            logger.info("restored %d scorer state(s) from checkpoint", len(states))
            return states
        except psycopg2.errors.UndefinedTable:
            logger.info("scorer_checkpoints table not found, starting fresh")
            return []
        except Exception as exc:
            logger.warning("checkpoint restore failed: %s", exc)
            return []


class NullCheckpointStore:
    """
    No-op checkpoint store used in tests / dry-run mode.

    Implements the same interface as CheckpointStore so service.py doesn't
    need to handle None.
    """

    def ensure_schema(self) -> None:
        pass

    def save(self, state: ScorerState) -> None:
        pass

    def save_all(self, states: List[ScorerState]) -> None:
        pass

    def restore_all(self) -> List[ScorerState]:
        return []

    def close(self) -> None:
        pass
