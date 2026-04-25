from __future__ import annotations

from datetime import datetime, date
from typing import List, Dict, Any

from fastapi import HTTPException, status

from app.core.exceptions import DatabaseError
from app.db.connection import get_db_connection, DatabaseConnection
from app.db.queries import (
    execute_query,
    SELECT_OC_PENDIENTES_APROBACION,
    SELECT_OC_CONSULTA_BASE,
    UPDATE_PSECAP00_APROBAR_OC,
    UPDATE_PDGOCO00_MARCAR_OC_APROBADA,
)


class OrdenCompraService:
    @staticmethod
    async def listar_pendientes_por_usuario(nombre_usuario: str) -> List[Dict[str, Any]]:
        # El SQL requiere el usuario dos veces (2do tramo del UNION ALL)
        return execute_query(SELECT_OC_PENDIENTES_APROBACION, (nombre_usuario, nombre_usuario))

    @staticmethod
    async def aprobar_orden(ctpdoc: str, ndocum: str, norden: int) -> Dict[str, Any]:
        """
        Aprueba una OC para un orden de aprobación específico (norden) y marca la OC como aprobada.
        Ejecuta ambas actualizaciones en una transacción.
        """
        faprob = datetime.now()

        with get_db_connection(DatabaseConnection.DEFAULT) as conn:
            try:
                cursor = conn.cursor()

                cursor.execute(UPDATE_PSECAP00_APROBAR_OC, (faprob, ctpdoc, ndocum, norden))
                rows_psecap = cursor.rowcount

                cursor.execute(UPDATE_PDGOCO00_MARCAR_OC_APROBADA, (ctpdoc, ndocum))
                rows_pdgoco = cursor.rowcount

                conn.commit()
                return {
                    "message": "Orden de compra aprobada",
                    "ctpdoc": ctpdoc,
                    "ndocum": ndocum,
                    "norden": norden,
                    "rows_psecap00": rows_psecap,
                    "rows_pdgoco00": rows_pdgoco,
                }
            except Exception as e:
                conn.rollback()
                # Mantener el estilo de errores existente
                raise DatabaseError(detail=f"Error aprobando orden de compra: {str(e)}")

    @staticmethod
    async def consultar_ordenes_compra(
        ctpdoc: str | None = None,
        ndocum: str | None = None,
        femisi: date | None = None,
        cliente: str | None = None,
        proveedor: str | None = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        query = SELECT_OC_CONSULTA_BASE
        params: list[Any] = []

        if ctpdoc:
            query += " AND g.ctpdoc = ?"
            params.append(ctpdoc)
        if ndocum:
            query += " AND g.ndocum = ?"
            params.append(ndocum)
        if femisi:
            query += " AND g.femisi = ?"
            params.append(femisi)
        if cliente:
            query += " AND c.drzsoc LIKE ?"
            params.append(f"%{cliente}%")
        if proveedor:
            query += " AND p.drzsoc LIKE ?"
            params.append(f"%{proveedor}%")

        query += " ORDER BY g.femisi DESC, g.ctpdoc, g.ndocum, d.norden"

        # Limitar resultados sin cambiar semántica (aplica post-query si TOP no está)
        result = execute_query(query, tuple(params))
        return result[:limit]

