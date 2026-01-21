# app/api/v1/endpoints/usuarios.py
"""
Módulo de endpoints para la gestión de usuarios del sistema.

Este módulo proporciona una API REST completa para operaciones CRUD sobre usuarios,
además de funcionalidades críticas de gestión de identidad como la asignación y
revocación de roles.

Características principales:
- Autenticación JWT con diferentes niveles de acceso:
    - Requerimiento de rol 'Administrador' para operaciones de gestión (Creación, Listado, Edición, Eliminación, Asignación de Roles).
    - Acceso para usuarios activos para consultar su propia información y roles.
- Validación robusta de datos de entrada (Pydantic).
- Implementación de paginación y búsqueda para listados de usuarios.
- Borrado lógico (`es_eliminado`) como mecanismo de eliminación.
- Manejo detallado de la relación Usuario-Rol (asignar/revocar).
- Gestión consistente de errores de negocio mediante CustomException.
"""

from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import List, Optional, Dict, Any

# Importar Schemas
from app.schemas.usuario import (
    UsuarioCreate,
    UsuarioUpdate,
    UsuarioRead,
    UsuarioReadWithRoles,
    PaginatedUsuarioResponse,
    PasswordReset,
    PasswordChange
)
from app.schemas.rol import RolRead
from app.schemas.usuario_rol import UsuarioRolRead

# Importar Servicios
from app.services.usuario_service import UsuarioService
from app.services.sync_service import SyncService

# Importar Excepciones personalizadas - CORREGIDO
from app.core.exceptions import CustomException

# Importar Dependencias de Autorización
from app.api.deps import get_current_active_user, RoleChecker

# Logging
from app.core.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

# Dependencia específica para requerir rol 'admin'
require_admin = RoleChecker(["Administrador"])


@router.get(
    "/",
    response_model=PaginatedUsuarioResponse,
    summary="Obtener lista paginada de usuarios",
    description="""
    Recupera una lista paginada de usuarios activos con sus roles.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de consulta:**
    - page: Número de página a mostrar (comienza en 1)
    - limit: Número máximo de usuarios por página (1-100)
    - search: Término opcional para buscar en nombre, apellido, correo o nombre_usuario
    
    **Respuestas:**
    - 200: Lista paginada recuperada exitosamente
    - 422: Parámetros de consulta inválidos
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def list_usuarios(
    page: int = Query(1, ge=1, description="Número de página a mostrar"),
    limit: int = Query(10, ge=1, le=100, description="Número de usuarios por página"),
    search: Optional[str] = Query(None, min_length=1, max_length=50, 
                                 description="Término de búsqueda opcional (nombre, apellido, correo, nombre_usuario)")
):
    """
    Endpoint para obtener una lista paginada y filtrada de usuarios activos.
    
    Args:
        page: Número de página solicitada
        limit: Límite de resultados por página
        search: Término opcional para búsqueda textual
        
    Returns:
        PaginatedUsuarioResponse: Respuesta paginada con usuarios y metadatos
        
    Raises:
        HTTPException: En caso de error en los parámetros o error interno
    """
    logger.info(
        f"Solicitud GET /usuarios/ recibida - "
        f"Paginación: page={page}, limit={limit}, "
        f"Búsqueda: '{search}'"
    )
    
    try:
        paginated_data = await UsuarioService.get_usuarios_paginated(
            page=page,
            limit=limit,
            search=search
        )
        
        logger.info(
            f"Lista paginada de usuarios recuperada - "
            f"Total: {paginated_data['total_usuarios']}, "
            f"Página: {paginated_data['pagina_actual']}"
        )
        return paginated_data
        
    except CustomException as ce:
        logger.warning(f"Error de negocio al listar usuarios: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception("Error inesperado en endpoint GET /usuarios/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al obtener la lista de usuarios."
        )


@router.post(
    "/",
    response_model=UsuarioRead,
    status_code=status.HTTP_201_CREATED,
    summary="Crear un nuevo usuario",
    description="""
    Crea un nuevo usuario en el sistema.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Validaciones:**
    - Nombre de usuario único
    - Correo electrónico único
    - Formato válido de contraseña (mínimo 8 caracteres, mayúscula, minúscula y número)
    - Campos obligatorios: nombre_usuario, correo, contrasena
    
    **Respuestas:**
    - 201: Usuario creado exitosamente
    - 409: Conflicto - Nombre de usuario o correo ya existen
    - 422: Error de validación en los datos de entrada
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def crear_usuario(usuario_in: UsuarioCreate):
    """
    Endpoint para crear un nuevo usuario en el sistema.
    
    Args:
        usuario_in: Datos validados del usuario a crear
        
    Returns:
        UsuarioRead: Usuario creado con todos sus datos incluyendo ID generado
        
    Raises:
        HTTPException: En caso de error de validación, conflicto o error interno
    """
    logger.info(f"Solicitud POST /usuarios/ recibida para crear usuario: '{usuario_in.nombre_usuario}'")
    
    try:
        usuario_dict = usuario_in.model_dump()
        created_usuario = await UsuarioService.crear_usuario(usuario_dict)
        
        logger.info(f"Usuario '{created_usuario['nombre_usuario']}' creado exitosamente con ID: {created_usuario['usuario_id']}")
        return created_usuario
        
    except CustomException as ce:
        logger.warning(f"Error de negocio al crear usuario '{usuario_in.nombre_usuario}': {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception("Error inesperado en endpoint POST /usuarios/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al crear el usuario."
        )


@router.get(
    "/{usuario_id}/",
    response_model=UsuarioReadWithRoles,
    summary="Obtener un usuario por ID",
    description="""
    Recupera los detalles completos de un usuario específico mediante su ID, incluyendo sus roles activos.
    
    **Permisos requeridos:**
    - Autenticación básica (usuario puede ver su propia información)
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario a consultar
    
    **Respuestas:**
    - 200: Usuario encontrado y devuelto
    - 404: Usuario no encontrado
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(get_current_active_user)]
)
async def read_usuario(usuario_id: int):
    """
    Endpoint para obtener los detalles completos de un usuario específico.
    
    Args:
        usuario_id: Identificador único del usuario a consultar
        
    Returns:
        UsuarioReadWithRoles: Detalles completos del usuario solicitado con roles
        
    Raises:
        HTTPException: Si el usuario no existe o hay error interno
    """
    logger.debug(f"Solicitud GET /usuarios/{usuario_id}/ recibida")
    
    try:
        usuario = await UsuarioService.obtener_usuario_por_id(usuario_id=usuario_id)
        
        if usuario is None:
            logger.warning(f"Usuario con ID {usuario_id} no encontrado")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Usuario con ID {usuario_id} no encontrado."
            )

        roles = await UsuarioService.obtener_roles_de_usuario(usuario_id=usuario_id)
        usuario_con_roles = UsuarioReadWithRoles(**usuario, roles=roles)
        
        logger.debug(f"Usuario ID {usuario_id} encontrado: '{usuario_con_roles.nombre_usuario}'")
        return usuario_con_roles
        
    except CustomException as ce:
        logger.error(f"Error de negocio obteniendo usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado obteniendo usuario {usuario_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al recuperar el usuario solicitado."
        )


@router.put(
    "/{usuario_id}/",
    response_model=UsuarioRead,
    summary="Actualizar un usuario existente",
    description="""
    Actualiza la información de un usuario existente mediante operación parcial (PATCH).
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario a actualizar
    
    **Validaciones:**
    - Al menos un campo debe ser proporcionado para actualizar
    - Si se actualiza nombre_usuario o correo, deben mantenerse únicos
    
    **Respuestas:**
    - 200: Usuario actualizado exitosamente
    - 400: Cuerpo de solicitud vacío
    - 404: Usuario no encontrado
    - 409: Conflicto - Nuevo nombre_usuario o correo ya existen
    - 422: Error de validación en los datos
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def actualizar_usuario(usuario_id: int, usuario_in: UsuarioUpdate):
    """
    Endpoint para actualizar parcialmente un usuario existente.
    
    Args:
        usuario_id: Identificador único del usuario a actualizar
        usuario_in: Campos a actualizar (actualización parcial)
        
    Returns:
        UsuarioRead: Usuario actualizado con los nuevos datos
        
    Raises:
        HTTPException: En caso de error de validación, no encontrado o conflicto
    """
    logger.info(f"Solicitud PUT /usuarios/{usuario_id}/ recibida para actualizar")
    
    # Validar que hay datos para actualizar
    update_data = usuario_in.model_dump(exclude_unset=True)
    if not update_data:
        logger.warning(f"Intento de actualizar usuario {usuario_id} sin datos")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Se debe proporcionar al menos un campo para actualizar el usuario."
        )
    
    try:
        updated_usuario = await UsuarioService.actualizar_usuario(usuario_id, update_data)
        
        logger.info(f"Usuario ID {usuario_id} actualizado exitosamente: '{updated_usuario['nombre_usuario']}'")
        return updated_usuario
        
    except CustomException as ce:
        logger.warning(f"Error de negocio al actualizar usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado en endpoint PUT /usuarios/{usuario_id}/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al actualizar el usuario."
        )


@router.delete(
    "/{usuario_id}/",
    response_model=dict,
    summary="Eliminar lógicamente un usuario",
    description="""
    Realiza un borrado lógico de un usuario estableciendo 'es_eliminado' a True y 'es_activo' a False.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario a eliminar
    
    **Notas:**
    - Operación reversible mediante actualización directa en BD
    - No elimina físicamente el registro
    - Desactiva automáticamente todas las asignaciones de roles del usuario
    
    **Respuestas:**
    - 200: Usuario eliminado exitosamente
    - 404: Usuario no encontrado
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def eliminar_usuario(usuario_id: int):
    """
    Endpoint para eliminar lógicamente un usuario (borrado lógico).
    
    Args:
        usuario_id: Identificador único del usuario a eliminar
        
    Returns:
        dict: Resultado de la eliminación con metadatos
        
    Raises:
        HTTPException: Si el usuario no existe o hay error interno
    """
    logger.info(f"Solicitud DELETE /usuarios/{usuario_id}/ recibida (eliminar lógicamente)")
    
    try:
        result = await UsuarioService.eliminar_usuario(usuario_id)
        
        logger.info(f"Usuario ID {usuario_id} eliminado lógicamente exitosamente")
        return result
        
    except CustomException as ce:
        logger.warning(f"Error de negocio al eliminar usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado en endpoint DELETE /usuarios/{usuario_id}/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al eliminar el usuario."
        )


@router.post(
    "/{usuario_id}/roles/{rol_id}/",
    response_model=UsuarioRolRead,
    status_code=status.HTTP_201_CREATED,
    summary="Asignar un rol a un usuario",
    description="""
    Asigna un rol específico a un usuario. Si la asignación existía inactiva, la reactiva.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario
    - rol_id: ID numérico del rol a asignar
    
    **Validaciones:**
    - El usuario debe existir y no estar eliminado
    - El rol debe existir y estar activo
    
    **Respuestas:**
    - 201: Rol asignado exitosamente
    - 404: Usuario o rol no encontrado
    - 409: El rol ya está asignado y activo
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def assign_rol_to_usuario(usuario_id: int, rol_id: int):
    """
    Endpoint para asignar un rol a un usuario.
    
    Args:
        usuario_id: Identificador único del usuario
        rol_id: Identificador único del rol a asignar
        
    Returns:
        UsuarioRolRead: Asignación usuario-rol creada o reactivada
        
    Raises:
        HTTPException: Si el usuario/rol no existen o hay error interno
    """
    logger.info(f"Solicitud POST /usuarios/{usuario_id}/roles/{rol_id}/ recibida")
    
    try:
        assignment = await UsuarioService.asignar_rol_a_usuario(usuario_id, rol_id)
        
        logger.info(f"Rol {rol_id} asignado exitosamente al usuario {usuario_id}")
        return assignment
        
    except CustomException as ce:
        logger.warning(f"Error de negocio asignando rol {rol_id} a usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado asignando rol {rol_id} a usuario {usuario_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al asignar el rol."
        )


@router.delete(
    "/{usuario_id}/roles/{rol_id}/",
    response_model=UsuarioRolRead,
    summary="Revocar un rol de un usuario",
    description="""
    Revoca (desactiva) la asignación de un rol específico para un usuario.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario
    - rol_id: ID numérico del rol a revocar
    
    **Notas:**
    - No elimina físicamente el registro, solo lo desactiva
    - Operación reversible mediante re-asignación
    
    **Respuestas:**
    - 200: Rol revocado exitosamente
    - 404: Asignación no encontrada
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def revoke_rol_from_usuario(usuario_id: int, rol_id: int):
    """
    Endpoint para revocar un rol de un usuario.
    
    Args:
        usuario_id: Identificador único del usuario
        rol_id: Identificador único del rol a revocar
        
    Returns:
        UsuarioRolRead: Asignación usuario-rol revocada
        
    Raises:
        HTTPException: Si la asignación no existe o hay error interno
    """
    logger.info(f"Solicitud DELETE /usuarios/{usuario_id}/roles/{rol_id}/ recibida")
    
    try:
        assignment = await UsuarioService.revocar_rol_de_usuario(usuario_id, rol_id)
        
        logger.info(f"Rol {rol_id} revocado exitosamente del usuario {usuario_id}")
        return assignment
        
    except CustomException as ce:
        logger.warning(f"Error de negocio revocando rol {rol_id} de usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado revocando rol {rol_id} de usuario {usuario_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al revocar el rol."
        )


@router.get(
    "/{usuario_id}/roles/",
    response_model=List[RolRead],
    summary="Obtener los roles de un usuario",
    description="""
    Devuelve una lista de todos los roles activos asignados a un usuario específico.
    
    **Permisos requeridos:**
    - Autenticación básica (usuario puede ver sus propios roles)
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario
    
    **Respuestas:**
    - 200: Lista de roles recuperada exitosamente
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(get_current_active_user)]
)
async def read_usuario_roles(usuario_id: int):
    """
    Endpoint para obtener los roles activos de un usuario.
    
    Args:
        usuario_id: Identificador único del usuario
        
    Returns:
        List[RolRead]: Lista de roles activos asignados al usuario
        
    Raises:
        HTTPException: En caso de error interno del servidor
    """
    logger.debug(f"Solicitud GET /usuarios/{usuario_id}/roles/ recibida")
    
    try:
        roles = await UsuarioService.obtener_roles_de_usuario(usuario_id)
        
        logger.debug(f"Roles del usuario {usuario_id} recuperados - Total: {len(roles)}")
        return roles
        
    except CustomException as ce:
        logger.error(f"Error de negocio obteniendo roles para usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado obteniendo roles para usuario {usuario_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al obtener los roles del usuario."
        )

# [ENDPOINT CORRECTO] - Consulta sin crear usuario
@router.get(
    "/consultar-perfil-externo/{codigo_trabajador}/",
    response_model=dict,
    summary="Consultar perfil externo sin crear usuario",
    description="""
    Consulta el nombre y apellido de un trabajador en el sistema externo
    usando su código de trabajador. NO crea ningún usuario, solo retorna
    los datos para pre-llenar el formulario.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de ruta:**
    - codigo_trabajador: Código del trabajador en el sistema externo
    
    **Flujo de uso:**
    1. Frontend muestra formulario de creación de usuario
    2. Usuario selecciona origen_datos = 'externo'
    3. Usuario ingresa código de trabajador
    4. Frontend llama a este endpoint
    5. Endpoint retorna nombre y apellido
    6. Frontend pre-llena los campos del formulario
    7. Usuario revisa y hace clic en "Guardar"
    8. Frontend llama a POST /usuarios/ con todos los datos
    
    **Respuestas:**
    - 200: Datos encontrados y retornados
    - 404: Trabajador no encontrado en sistema externo
    - 503: Error al conectar con sistema externo
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def consultar_perfil_externo(codigo_trabajador: str):
    """
    Endpoint para consultar perfil externo sin crear usuario.
    Usado para pre-llenar formularios en el frontend.
    
    Args:
        codigo_trabajador: Código del trabajador en sistema externo
        
    Returns:
        dict: Nombre y apellido del trabajador
        
    Raises:
        HTTPException: Si no se encuentra o hay error de conexión
    """
    logger.info(f"Solicitud GET /usuarios/consultar-perfil-externo/{codigo_trabajador}/ recibida")
    
    # Validar formato básico del código
    if not codigo_trabajador or codigo_trabajador.strip() == '':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El código de trabajador no puede estar vacío"
        )
    
    codigo_trabajador = codigo_trabajador.strip()
    
    try:
        # 🔍 Consultar BD externa
        perfil = await SyncService.obtener_perfil_externo(codigo_trabajador)
        
        if not perfil:
            logger.warning(f"Trabajador con código {codigo_trabajador} no encontrado en sistema externo")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No se encontró un trabajador con el código '{codigo_trabajador}' en el sistema externo"
            )
        
        # ✅ Validar que tenga al menos nombre o apellido
        nombre = perfil.get('nombre', '').strip()
        apellido = perfil.get('apellido', '').strip()
        dni_trabajador = perfil.get('dni_trabajador', '').strip()
        
        if not nombre and not apellido:
            logger.error(f"Perfil externo para código {codigo_trabajador} no contiene nombre ni apellido")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="El perfil encontrado no contiene información de nombre o apellido válida"
            )
        
        logger.info(f"Perfil externo encontrado: {nombre} {apellido}")
        
        return {
            "codigo_trabajador": codigo_trabajador,
            "nombre": nombre,
            "apellido": apellido,
            "dni_trabajador": dni_trabajador,
            "mensaje": "Perfil encontrado en sistema externo"
        }
        
    except HTTPException:
        # Re-lanzar HTTPExceptions ya manejadas
        raise
    except CustomException as ce:
        logger.warning(f"Error al consultar perfil externo: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code,
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado consultando perfil externo")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al consultar el sistema externo"
        )


@router.post(
    "/{usuario_id}/reset-password/",
    response_model=dict,
    summary="Resetear contraseña de un usuario (Administrador)",
    description=""" 
    Permite a un administrador resetear la contraseña de cualquier usuario
    sin necesidad de conocer la contraseña actual.
    
    **Permisos requeridos:**
    - Rol 'Administrador'
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario cuya contraseña se reseteará
    
    **Validaciones:**
    - El usuario debe existir y no estar eliminado
    - La nueva contraseña debe cumplir con las políticas de seguridad:
      - Mínimo 8 caracteres
      - Al menos una letra mayúscula
      - Al menos una letra minúscula
      - Al menos un número
    
    **Respuestas:**
    - 200: Contraseña reseteada exitosamente
    - 404: Usuario no encontrado
    - 422: Error de validación en la nueva contraseña
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(require_admin)]
)
async def resetear_contrasena_usuario(usuario_id: int, password_reset: PasswordReset):
    """
    Endpoint para resetear la contraseña de un usuario (solo administradores).
    
    Args:
        usuario_id: Identificador único del usuario
        password_reset: Datos con la nueva contraseña
        
    Returns:
        dict: Resultado del reset con metadatos
        
    Raises:
        HTTPException: En caso de error de validación, no encontrado o error interno
    """
    logger.info(f"Solicitud POST /usuarios/{usuario_id}/reset-password/ recibida")
    
    try:
        result = await UsuarioService.resetear_contrasena(
            usuario_id=usuario_id,
            nueva_contrasena=password_reset.nueva_contrasena
        )
        
        logger.info(f"Contraseña reseteada exitosamente para usuario ID {usuario_id}")
        return result
        
    except CustomException as ce:
        logger.warning(f"Error de negocio al resetear contraseña para usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado en endpoint POST /usuarios/{usuario_id}/reset-password/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al resetear la contraseña."
        )


@router.post(
    "/{usuario_id}/change-password/",
    response_model=dict,
    summary="Cambiar contraseña propia",
    description="""
    Permite a un usuario cambiar su propia contraseña proporcionando
    la contraseña actual y la nueva contraseña.
    
    **Permisos requeridos:**
    - Autenticación (el usuario solo puede cambiar su propia contraseña)
    
    **Parámetros de ruta:**
    - usuario_id: ID numérico del usuario (debe coincidir con el usuario autenticado)
    
    **Validaciones:**
    - El usuario debe existir y no estar eliminado
    - La contraseña actual debe ser correcta
    - La nueva contraseña debe ser diferente a la actual
    - La nueva contraseña debe cumplir con las políticas de seguridad:
      - Mínimo 8 caracteres
      - Al menos una letra mayúscula
      - Al menos una letra minúscula
      - Al menos un número
    
    **Respuestas:**
    - 200: Contraseña cambiada exitosamente
    - 400: Contraseña actual incorrecta o nueva contraseña igual a la actual
    - 403: Intento de cambiar contraseña de otro usuario
    - 404: Usuario no encontrado
    - 422: Error de validación en la nueva contraseña
    - 500: Error interno del servidor
    """,
    dependencies=[Depends(get_current_active_user)]
)
async def cambiar_contrasena_propia(
    usuario_id: int, 
    password_change: PasswordChange,
    current_user: UsuarioReadWithRoles = Depends(get_current_active_user)
):
    """
    Endpoint para que un usuario cambie su propia contraseña.
    
    Args:
        usuario_id: Identificador único del usuario
        password_change: Datos con contraseña actual y nueva contraseña
        current_user: Usuario autenticado actual
        
    Returns:
        dict: Resultado del cambio con metadatos
        
    Raises:
        HTTPException: En caso de error de validación, no autorizado o error interno
    """
    logger.info(f"Solicitud POST /usuarios/{usuario_id}/change-password/ recibida")
    
    # 🔒 VERIFICAR QUE EL USUARIO SOLO PUEDE CAMBIAR SU PROPIA CONTRASEÑA
    if current_user.usuario_id != usuario_id:
        logger.warning(
            f"Intento de cambiar contraseña de otro usuario. "
            f"Usuario autenticado: {current_user.usuario_id}, "
            f"Usuario solicitado: {usuario_id}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes permisos para cambiar la contraseña de otro usuario."
        )
    
    try:
        result = await UsuarioService.cambiar_contrasena_propia(
            usuario_id=usuario_id,
            contrasena_actual=password_change.contrasena_actual,
            nueva_contrasena=password_change.nueva_contrasena
        )
        
        logger.info(f"Contraseña cambiada exitosamente para usuario ID {usuario_id}")
        return result
        
    except CustomException as ce:
        logger.warning(f"Error de negocio al cambiar contraseña para usuario {usuario_id}: {ce.detail}")
        raise HTTPException(
            status_code=ce.status_code, 
            detail=ce.detail
        )
    except Exception as e:
        logger.exception(f"Error inesperado en endpoint POST /usuarios/{usuario_id}/change-password/")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor al cambiar la contraseña."
        )