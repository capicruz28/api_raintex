from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class OrdenCompraPendienteRead(BaseModel):
    ctpdoc: str
    ndocum: str
    proveedor: Optional[str] = None
    femisi: Optional[date] = None
    fentre: Optional[date] = None
    itotal: float = 0.0
    cmoned: Optional[str] = None
    norden: int
    observacion: Optional[str] = None
    cliente: Optional[str] = None
    tipo_documento: Optional[str] = None
    articulo: Optional[str] = None
    descripcion_articulo: Optional[str] = None
    cantidad_solicitada: Optional[float] = None
    precio_unitario: Optional[float] = None
    orden_detalle: Optional[int] = None
    orden_trabajo: Optional[str] = None
    usuario_creacion: Optional[str] = None
    forma_pago: Optional[str] = None


class AprobarOrdenCompraRequest(BaseModel):
    ctpdoc: str = Field(..., min_length=1)
    ndocum: str = Field(..., min_length=1)
    norden: int = Field(..., ge=1)


class OrdenCompraConsultaItemRead(BaseModel):
    ctpdoc: str
    ndocum: str
    proveedor: Optional[str] = None
    femisi: Optional[date] = None
    fentre: Optional[date] = None
    itotal: float = 0.0
    cmoned: Optional[str] = None
    observacion: Optional[str] = None
    cliente: Optional[str] = None
    tipo_documento: Optional[str] = None
    articulo: Optional[str] = None
    descripcion_articulo: Optional[str] = None
    cantidad_solicitado: Optional[float] = None
    precio_unitario: Optional[float] = None
    orden_detalle: Optional[int] = None
    orden_trabajo: Optional[str] = None
    usuario_creacion: Optional[str] = None
    forma_pago: Optional[str] = None

