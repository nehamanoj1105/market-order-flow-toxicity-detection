"""Audit logging helper for persistent UC-04 audit records."""

from typing import Any, Optional
from app.core.database import write_audit_log

__all__ = ["write_audit_log"]
