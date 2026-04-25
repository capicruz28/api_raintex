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
    citems: Optional[str] = None
    ditems: Optional[str] = None
    qsolic: Optional[float] = None
    ipruni: Optional[float] = None
    norden: Optional[int] = None

