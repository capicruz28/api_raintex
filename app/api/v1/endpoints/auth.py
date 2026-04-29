# app/api/v1/endpoints/auth.py
"""
Módulo de endpoints para la gestión de la autenticación de usuarios (Login, Logout, Refresh Token).

Este módulo maneja el flujo de autenticación basado en JWT y cookies seguras.

Características principales:
- **Login:** Verifica credenciales, genera Access Token y Refresh Token (cookie HttpOnly + cuerpo JSON).
- **Me:** Permite al usuario obtener su información y roles usando el Access Token.
- **Refresh:** Renueva Access Token desde cookie (web) o body JSON (móvil), con rotación y persistencia en BD.
- **Logout:** Revoca refresh en BD y elimina cookie.
"""
from typing import Dict, Literal, Optional

from fastapi import APIRouter, HTTPException, status, Depends, Response, Request, Body

from fastapi.security import OAuth2PasswordRequestForm

from app.schemas.auth import (
    Token,
    UserDataWithRoles,
    RefreshTokenBody,
    LogoutRequestBody,
)
from app.schemas.usuario import UsuarioReadWithRoles, PasswordChange
from app.core.auth import (
    authenticate_user,
    create_access_token,
    create_refresh_token_with_expiry,
    get_current_user,
    validate_refresh_token_for_rotation,
)
from app.core.config import settings
from app.core.logging_config import get_logger
from app.core.exceptions import CustomException
from app.core.refresh_token_service import (
    save_refresh_token,
    revoke_refresh_token,
    hash_token,
    fetch_active_refresh_row,
    ClientType,
)
from app.services.usuario_service import UsuarioService
from app.api.deps import get_current_active_user

router = APIRouter()
logger = get_logger(__name__)


def _resolve_client_type(request: Request) -> ClientType:
    raw = (request.headers.get("X-Client-Type") or "web").strip().lower()
    if raw == "mobile":
        return "mobile"
    return "web"


# ----------------------------------------------------------------------
# --- Endpoint para Login ---
# ----------------------------------------------------------------------
@router.post(
    "/login/",
    response_model=Token,
    summary="Autenticar usuario y obtener token",
    description="""
    Verifica credenciales (nombre de usuario/email y contraseña) proporcionadas mediante formulario `OAuth2PasswordRequestForm`. 
    Genera un **Access Token** y **Refresh Token** (también en cookie HttpOnly para el flujo web).
    Retorna los datos básicos del usuario, incluyendo sus roles.
    """,
)
async def login(
    request: Request,
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    usuario_service = UsuarioService()
    try:
        user_base_data = await authenticate_user(form_data.username, form_data.password)

        user_id = user_base_data.get("usuario_id")
        user_role_names = await usuario_service.get_user_role_names(user_id=user_id)

        user_full_data = {**user_base_data, "roles": user_role_names}

        access_token = create_access_token(data={"sub": form_data.username})
        refresh_token, expires_at = create_refresh_token_with_expiry(
            data={"sub": form_data.username}
        )

        client_type = _resolve_client_type(request)
        save_refresh_token(
            int(user_id),
            refresh_token,
            expires_at,
            client_type,
            request.client.host if request.client else None,
            request.headers.get("user-agent"),
        )

        response.set_cookie(
            key=settings.REFRESH_COOKIE_NAME,
            value=refresh_token,
            httponly=True,
            secure=settings.COOKIE_SECURE,
            samesite=settings.COOKIE_SAMESITE,
            max_age=settings.REFRESH_COOKIE_MAX_AGE,
            path="/",
        )

        logger.info("Usuario %s autenticado exitosamente", form_data.username)

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "user_data": user_full_data,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Error inesperado en /login/ para usuario %s: %s",
            form_data.username,
            str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocurrió un error inesperado durante el proceso de login.",
        )


# ----------------------------------------------------------------------
# --- Endpoint para Obtener Usuario Actual (Me) ---
# ----------------------------------------------------------------------
@router.get(
    "/me/",
    response_model=UserDataWithRoles,
    summary="Obtener usuario actual",
    description="""
    Retorna los datos completos del usuario autenticado, incluyendo roles y metadatos. 
    Requiere un **Access Token válido** en el header `Authorization: Bearer <token>`.
    """,
)
async def get_me(current_user: dict = Depends(get_current_user)):
    logger.info(
        "Solicitud /me/ recibida para usuario: %s",
        current_user.get("nombre_usuario"),
    )
    try:
        usuario_service = UsuarioService()
        user_id = current_user.get("usuario_id")
        user_role_names = await usuario_service.get_user_role_names(user_id=user_id)
        user_full_data = {**current_user, "roles": user_role_names}
        return user_full_data
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /me/: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error obteniendo datos del usuario",
        )


# ----------------------------------------------------------------------
# --- Endpoint para Debug de Roles (útil para verificar roles) ---
# ----------------------------------------------------------------------
@router.get(
    "/me/roles/",
    response_model=Dict,
    summary="Obtener roles detallados del usuario actual",
    description="""
    Retorna información detallada de los roles del usuario autenticado.
    Útil para debugging y verificar qué roles tiene asignados.
    """,
)
async def get_my_roles(
    current_user: UsuarioReadWithRoles = Depends(get_current_active_user),
):
    try:
        roles_info = []
        for role in current_user.roles:
            roles_info.append(
                {
                    "rol_id": role.rol_id,
                    "nombre": role.nombre,
                    "nombre_normalizado": role.nombre.strip().lower(),
                    "descripcion": role.descripcion,
                    "es_activo": role.es_activo,
                }
            )

        return {
            "usuario_id": current_user.usuario_id,
            "nombre_usuario": current_user.nombre_usuario,
            "roles": roles_info,
            "nombres_roles": [role.nombre for role in current_user.roles],
            "tiene_superadmin": any(
                role.nombre.strip().lower() == "superadministrador"
                for role in current_user.roles
            ),
        }
    except Exception as e:
        logger.exception("Error obteniendo roles detallados: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error obteniendo roles del usuario",
        )


# ----------------------------------------------------------------------
# --- Endpoint para Refrescar Access Token ---
# ----------------------------------------------------------------------
@router.post(
    "/refresh/",
    summary="Refrescar Access Token",
    description="""
    Genera un nuevo Access Token usando el Refresh Token en **cookie HttpOnly** (web)
    o en el **cuerpo JSON** (móvil). Rota el refresh token y persiste en BD.
    """,
)
async def refresh_access_token(
    request: Request,
    response: Response,
    body: Optional[RefreshTokenBody] = Body(None),
):
    cookie_token = request.cookies.get(settings.REFRESH_COOKIE_NAME)
    raw: Optional[str] = None
    from_cookie = False
    if cookie_token:
        raw = cookie_token
        from_cookie = True
    elif body is not None and body.refresh_token:
        raw = body.refresh_token.strip()
        from_cookie = False

    if not raw:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No refresh token provided",
        )

    try:
        user, old_token_id = validate_refresh_token_for_rotation(raw)
        revoke_refresh_token(old_token_id)

        username = user["nombre_usuario"]
        new_access_token = create_access_token(data={"sub": username})
        new_refresh_token, expires_at = create_refresh_token_with_expiry(
            data={"sub": username}
        )

        client_type: Literal["web", "mobile"] = "web" if from_cookie else "mobile"
        save_refresh_token(
            int(user["usuario_id"]),
            new_refresh_token,
            expires_at,
            client_type,
            request.client.host if request.client else None,
            request.headers.get("user-agent"),
        )

        if from_cookie:
            response.set_cookie(
                key=settings.REFRESH_COOKIE_NAME,
                value=new_refresh_token,
                httponly=True,
                secure=settings.COOKIE_SECURE,
                samesite=settings.COOKIE_SAMESITE,
                max_age=settings.REFRESH_COOKIE_MAX_AGE,
                path="/",
            )
            logger.info("Refresh vía cookie completado para usuario %s", username)
            return {
                "access_token": new_access_token,
                "token_type": "bearer",
            }

        logger.info("Refresh vía body completado para usuario %s", username)
        return {
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error en /refresh/: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al refrescar el token",
        )


# ----------------------------------------------------------------------
# --- Endpoint para Cerrar Sesión (Logout) ---
# ----------------------------------------------------------------------
@router.post(
    "/logout/",
    summary="Cerrar sesión",
    description="""
    Revoca el refresh token en servidor (cookie y/o cuerpo JSON) y elimina la cookie.
    """,
)
async def logout(
    request: Request,
    response: Response,
    body: Optional[LogoutRequestBody] = Body(None),
):
    raw: Optional[str] = request.cookies.get(settings.REFRESH_COOKIE_NAME)
    if not raw and body is not None and body.refresh_token:
        raw = body.refresh_token.strip()

    if raw:
        try:
            from jose import jwt, JWTError

            payload = jwt.decode(
                raw,
                settings.SECRET_KEY,
                algorithms=[settings.ALGORITHM],
            )
            if payload.get("type") == "refresh":
                row = fetch_active_refresh_row(hash_token(raw))
                if row:
                    revoke_refresh_token(int(row["token_id"]))
        except JWTError:
            pass

    response.delete_cookie(
        key=settings.REFRESH_COOKIE_NAME,
        path="/",
        samesite=settings.COOKIE_SAMESITE,
        secure=settings.COOKIE_SECURE,
        httponly=True,
    )
    logger.info("Sesión cerrada (logout)")
    return {"message": "Sesión cerrada exitosamente"}


# ----------------------------------------------------------------------
# --- Endpoint para Cambiar Contraseña Propia ---
# ----------------------------------------------------------------------
@router.post(
    "/change-password/",
    response_model=dict,
    summary="Cambiar contraseña propia",
    description="""
    Permite a un usuario autenticado cambiar su propia contraseña proporcionando
    la contraseña actual y la nueva contraseña.
    """,
)
async def change_password(
    password_change: PasswordChange,
    current_user: UsuarioReadWithRoles = Depends(get_current_active_user),
):
    logger.info(
        "Solicitud POST /auth/change-password/ recibida para usuario ID: %s",
        current_user.usuario_id,
    )

    try:
        usuario_service = UsuarioService()
        result = await usuario_service.cambiar_contrasena_propia(
            usuario_id=current_user.usuario_id,
            contrasena_actual=password_change.contrasena_actual,
            nueva_contrasena=password_change.nueva_contrasena,
        )

        logger.info(
            "Contraseña cambiada exitosamente para usuario ID %s",
            current_user.usuario_id,
        )
        return result

    except CustomException as ce:
        logger.warning(
            "Error de negocio al cambiar contraseña para usuario %s: %s",
            current_user.usuario_id,
            ce.detail,
        )
        raise HTTPException(status_code=ce.status_code, detail=ce.detail)
    except Exception as e:
        logger.exception("Error inesperado en endpoint POST /auth/change-password/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al cambiar la contraseña.",
        )
