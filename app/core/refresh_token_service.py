# app/core/refresh_token_service.py
"""Persistencia y validación de refresh tokens (hash en BD, rotación, revocación)."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, Literal, Optional

from app.db.queries import (
    REFRESH_TOKEN_INSERT,
    REFRESH_TOKEN_SELECT_ACTIVE_BY_HASH,
    REFRESH_TOKEN_REVOKE_BY_ID,
    execute_auth_query,
    execute_insert,
    execute_update,
)

logger = logging.getLogger(__name__)

ClientType = Literal["web", "mobile"]


def hash_token(raw_token: str) -> str:
    """SHA-256 hex del token en bruto (no almacenar JWT en claro)."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def save_refresh_token(
    usuario_id: int,
    raw_token: str,
    expires_at: datetime,
    client_type: ClientType,
    ip_address: Optional[str],
    user_agent: Optional[str],
) -> int:
    """Inserta fila en refresh_tokens. Devuelve token_id."""
    token_hash = hash_token(raw_token)
    params = (
        usuario_id,
        token_hash,
        expires_at,
        client_type,
        ip_address or "",
        user_agent or "",
    )
    result = execute_insert(REFRESH_TOKEN_INSERT, params)
    token_id = result.get("token_id")
    if token_id is None:
        raise RuntimeError("No se pudo obtener token_id tras insertar refresh token")
    return int(token_id)


def revoke_refresh_token(token_id: int) -> None:
    """Marca el refresh token como revocado."""
    execute_update(REFRESH_TOKEN_REVOKE_BY_ID, (token_id,))


def fetch_active_refresh_row(token_hash: str) -> Optional[Dict[str, Any]]:
    """Obtiene fila activa y no expirada por hash."""
    return execute_auth_query(REFRESH_TOKEN_SELECT_ACTIVE_BY_HASH, (token_hash,))
