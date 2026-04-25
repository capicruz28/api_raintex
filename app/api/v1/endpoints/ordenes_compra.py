from typing import List, Dict, Any, Optional
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_current_active_user
from app.schemas.usuario import UsuarioReadWithRoles
from app.schemas.orden_compra import (
    OrdenCompraPendienteRead,
    AprobarOrdenCompraRequest,
    OrdenCompraConsultaItemRead,
)
from app.services.orden_compra_service import OrdenCompraService
from app.core.exceptions import CustomException
from app.core.logging_config import get_logger


logger = get_logger(__name__)
router = APIRouter()


@router.get(
    "/pendientes",
    response_model=List[OrdenCompraPendienteRead],
    summary="Listar órdenes de compra pendientes de aprobación",
    description="Devuelve las órdenes de compra pendientes de aprobación para el usuario autenticado.",
    dependencies=[Depends(get_current_active_user)],
)
async def listar_oc_pendientes(
    current_user: UsuarioReadWithRoles = Depends(get_current_active_user),
):
    try:
        username = current_user.nombre_usuario
        return await OrdenCompraService.listar_pendientes_por_usuario(username)
    except HTTPException:
        raise
    except CustomException as ce:
        raise HTTPException(status_code=ce.status_code, detail=ce.detail)
    except Exception as e:
        logger.exception(f"Error listando OCs pendientes: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al obtener órdenes de compra pendientes",
        )


@router.post(
    "/aprobar",
    summary="Aprobar una orden de compra",
    description="Aprueba una orden de compra (según norden) y actualiza pdgoco00.",
    dependencies=[Depends(get_current_active_user)],
)
async def aprobar_oc(
    payload: AprobarOrdenCompraRequest,
    current_user: UsuarioReadWithRoles = Depends(get_current_active_user),
):
    try:
        # Nota: la validación de si el usuario realmente puede aprobar esa OC
        # se delega al SQL de pendientes (cliente consumirá solo lo que ve).
        return await OrdenCompraService.aprobar_orden(
            ctpdoc=payload.ctpdoc,
            ndocum=payload.ndocum,
            norden=payload.norden,
        )
    except HTTPException:
        raise
    except CustomException as ce:
        raise HTTPException(status_code=ce.status_code, detail=ce.detail)
    except Exception as e:
        logger.exception(f"Error aprobando OC: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al aprobar la orden de compra",
        )


@router.get(
    "/consulta",
    response_model=List[OrdenCompraConsultaItemRead],
    summary="Consultar órdenes de compra (con filtros opcionales)",
    description="Consulta órdenes de compra con filtros opcionales. Solo filtra si se envían parámetros.",
    dependencies=[Depends(get_current_active_user)],
)
async def consultar_ordenes_compra(
    ctpdoc: Optional[str] = None,
    ndocum: Optional[str] = None,
    femisi: Optional[date] = None,
    cliente: Optional[str] = None,
    proveedor: Optional[str] = None,
    limit: int = 200,
    current_user: UsuarioReadWithRoles = Depends(get_current_active_user),
):
    try:
        return await OrdenCompraService.consultar_ordenes_compra(
            ctpdoc=ctpdoc,
            ndocum=ndocum,
            femisi=femisi,
            cliente=cliente,
            proveedor=proveedor,
            limit=limit,
        )
    except HTTPException:
        raise
    except CustomException as ce:
        raise HTTPException(status_code=ce.status_code, detail=ce.detail)
    except Exception as e:
        logger.exception(f"Error consultando OCs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al consultar órdenes de compra",
        )

