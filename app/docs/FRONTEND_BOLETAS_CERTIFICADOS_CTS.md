# 📋 Documentación Frontend - Boletas de Pago y Certificados CTS

## 🔗 Endpoints Disponibles

### 1. Obtener Boleta de Pago

**URL:** `GET /api/v1/vacaciones-permisos-mobile/boleta-pago`

**URL Completa:** `https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/boleta-pago`

---

### 2. Obtener Certificado CTS

**URL:** `GET /api/v1/vacaciones-permisos-mobile/certificado-cts`

**URL Completa:** `https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/certificado-cts`

---

## 🔐 Autenticación

Ambos endpoints requieren autenticación mediante **Access Token JWT**.

### Headers Requeridos

```http
Authorization: Bearer <tu_access_token>
Content-Type: application/json
```

**Ejemplo:**
```http
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Content-Type: application/json
```

---

## 📤 Endpoint 1: Obtener Boleta de Pago

### Request

**Método:** `GET`

**URL:** `/api/v1/vacaciones-permisos-mobile/boleta-pago`

**Query Parameters:**

| Parámetro | Tipo | Requerido | Descripción | Ejemplo |
|-----------|------|-----------|-------------|---------|
| `anio` | string | ✅ Sí | Año de la boleta (formato: YYYY) | `2025` |
| `mes` | string | ✅ Sí | Mes de la boleta (formato: MM) | `09` |

**Ejemplo de URL:**
```
GET /api/v1/vacaciones-permisos-mobile/boleta-pago?anio=2025&mes=09
```

---

### ✅ Respuesta Exitosa (200 OK)

```json
{
  "codigo_trabajador": "PR011959",
  "anio": "2025",
  "mes": "09",
  "archivo_pdf_base64": "JVBERi0xLjQKJeLjz9MKMy...",
  "nombre_archivo": "boleta_PR011959_2025_09.pdf"
}
```

**Campos de Respuesta:**

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `codigo_trabajador` | string | Código del trabajador |
| `anio` | string | Año de la boleta |
| `mes` | string | Mes de la boleta |
| `archivo_pdf_base64` | string | Archivo PDF codificado en base64 |
| `nombre_archivo` | string | Nombre sugerido para guardar el archivo |

---

### ❌ Respuestas de Error

#### 404 Not Found - Boleta no encontrada

```json
{
  "detail": "No se encontró boleta de pago para el año 2025 y mes 09. Verifique que la boleta exista en el sistema.",
  "error_code": "BOLETA_NOT_FOUND"
}
```

#### 404 Not Found - Sin archivo PDF

```json
{
  "detail": "La boleta de pago para el año 2025 y mes 09 no tiene archivo PDF asociado. Contacte al área de recursos humanos.",
  "error_code": "BOLETA_SIN_ARCHIVO"
}
```

#### 401 Unauthorized - Token inválido/expirado

```json
{
  "detail": "No se pudieron validar las credenciales"
}
```

#### 500 Internal Server Error - Error de procesamiento

```json
{
  "detail": "Error al procesar el archivo PDF de la boleta",
  "error_code": "BOLETA_CONVERSION_ERROR"
}
```

---

## 📤 Endpoint 2: Obtener Certificado CTS

### Request

**Método:** `GET`

**URL:** `/api/v1/vacaciones-permisos-mobile/certificado-cts`

**Query Parameters:**

| Parámetro | Tipo | Requerido | Descripción | Ejemplo |
|-----------|------|-----------|-------------|---------|
| `anio` | string | ✅ Sí | Año del certificado (formato: YYYY) | `2024` |

**Ejemplo de URL:**
```
GET /api/v1/vacaciones-permisos-mobile/certificado-cts?anio=2024
```

---

### ✅ Respuesta Exitosa (200 OK)

```json
{
  "codigo_trabajador": "PR011959",
  "anio": "2024",
  "mes": null,
  "archivo_pdf_base64": "JVBERi0xLjQKJeLjz9MKMy...",
  "nombre_archivo": "certificado_cts_PR011959_2024.pdf"
}
```

**Campos de Respuesta:**

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `codigo_trabajador` | string | Código del trabajador |
| `anio` | string | Año del certificado |
| `mes` | string \| null | Mes del certificado (puede ser null) |
| `archivo_pdf_base64` | string | Archivo PDF codificado en base64 |
| `nombre_archivo` | string | Nombre sugerido para guardar el archivo |

---

### ❌ Respuestas de Error

#### 404 Not Found - Certificado no encontrado

```json
{
  "detail": "No se encontró certificado CTS para el año 2024. Verifique que el certificado exista en el sistema.",
  "error_code": "CERTIFICADO_CTS_NOT_FOUND"
}
```

#### 404 Not Found - Sin archivo PDF

```json
{
  "detail": "El certificado CTS para el año 2024 no tiene archivo PDF asociado. Contacte al área de recursos humanos.",
  "error_code": "CERTIFICADO_SIN_ARCHIVO"
}
```

#### 401 Unauthorized - Token inválido/expirado

```json
{
  "detail": "No se pudieron validar las credenciales"
}
```

#### 500 Internal Server Error - Error de procesamiento

```json
{
  "detail": "Error al procesar el archivo PDF del certificado",
  "error_code": "CERTIFICADO_CONVERSION_ERROR"
}
```

---

## 💻 Ejemplos de Implementación

### 🌐 JavaScript/TypeScript (Fetch API)

#### Obtener Boleta de Pago

```typescript
interface BoletaResponse {
  codigo_trabajador: string;
  anio: string;
  mes: string;
  archivo_pdf_base64: string;
  nombre_archivo: string;
}

interface ErrorResponse {
  detail: string;
  error_code?: string;
}

async function obtenerBoletaPago(
  accessToken: string,
  anio: string,
  mes: string
): Promise<BoletaResponse | null> {
  try {
    const response = await fetch(
      `https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/boleta-pago?anio=${anio}&mes=${mes}`,
      {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );

    if (response.status === 404) {
      const error: ErrorResponse = await response.json();
      console.warn('Boleta no encontrada:', error.detail);
      // Mostrar mensaje al usuario
      alert(error.detail);
      return null;
    }

    if (!response.ok) {
      throw new Error(`Error HTTP: ${response.status}`);
    }

    const data: BoletaResponse = await response.json();
    return data;
    
  } catch (error) {
    console.error('Error obteniendo boleta:', error);
    alert('Error al obtener la boleta de pago');
    return null;
  }
}

// Función para descargar el PDF desde base64
function descargarPDF(base64: string, nombreArchivo: string) {
  // Convertir base64 a blob
  const byteCharacters = atob(base64);
  const byteNumbers = new Array(byteCharacters.length);
  for (let i = 0; i < byteCharacters.length; i++) {
    byteNumbers[i] = byteCharacters.charCodeAt(i);
  }
  const byteArray = new Uint8Array(byteNumbers);
  const blob = new Blob([byteArray], { type: 'application/pdf' });

  // Crear URL y descargar
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = nombreArchivo;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// Función para visualizar el PDF en el navegador
function visualizarPDF(base64: string) {
  const byteCharacters = atob(base64);
  const byteNumbers = new Array(byteCharacters.length);
  for (let i = 0; i < byteCharacters.length; i++) {
    byteNumbers[i] = byteCharacters.charCodeAt(i);
  }
  const byteArray = new Uint8Array(byteNumbers);
  const blob = new Blob([byteArray], { type: 'application/pdf' });
  
  const url = URL.createObjectURL(blob);
  window.open(url, '_blank');
}

// Uso
const boleta = await obtenerBoletaPago(accessToken, '2025', '09');
if (boleta) {
  // Descargar PDF
  descargarPDF(boleta.archivo_pdf_base64, boleta.nombre_archivo);
  
  // O visualizar en nueva pestaña
  // visualizarPDF(boleta.archivo_pdf_base64);
}
```

#### Obtener Certificado CTS

```typescript
interface CertificadoCTSResponse {
  codigo_trabajador: string;
  anio: string;
  mes: string | null;
  archivo_pdf_base64: string;
  nombre_archivo: string;
}

async function obtenerCertificadoCTS(
  accessToken: string,
  anio: string
): Promise<CertificadoCTSResponse | null> {
  try {
    const response = await fetch(
      `https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/certificado-cts?anio=${anio}`,
      {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );

    if (response.status === 404) {
      const error: ErrorResponse = await response.json();
      console.warn('Certificado no encontrado:', error.detail);
      alert(error.detail);
      return null;
    }

    if (!response.ok) {
      throw new Error(`Error HTTP: ${response.status}`);
    }

    const data: CertificadoCTSResponse = await response.json();
    return data;
    
  } catch (error) {
    console.error('Error obteniendo certificado:', error);
    alert('Error al obtener el certificado CTS');
    return null;
  }
}

// Uso
const certificado = await obtenerCertificadoCTS(accessToken, '2024');
if (certificado) {
  descargarPDF(certificado.archivo_pdf_base64, certificado.nombre_archivo);
}
```

---

### ⚛️ React (con Axios)

```typescript
import axios from 'axios';

// Hook personalizado para obtener boleta
const useBoletaPago = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const obtenerBoleta = async (anio: string, mes: string, accessToken: string) => {
    setLoading(true);
    setError(null);

    try {
      const response = await axios.get(
        '/api/v1/vacaciones-permisos-mobile/boleta-pago',
        {
          params: { anio, mes },
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
          }
        }
      );

      return response.data;
    } catch (err: any) {
      if (err.response?.status === 404) {
        const errorMsg = err.response.data.detail;
        setError(errorMsg);
        return null;
      }
      setError('Error al obtener la boleta');
      return null;
    } finally {
      setLoading(false);
    }
  };

  return { obtenerBoleta, loading, error };
};

// Componente React
const BoletaPagoComponent = () => {
  const [anio, setAnio] = useState('2025');
  const [mes, setMes] = useState('09');
  const accessToken = localStorage.getItem('access_token') || '';
  const { obtenerBoleta, loading, error } = useBoletaPago();

  const handleDescargar = async () => {
    const boleta = await obtenerBoleta(anio, mes, accessToken);
    if (boleta) {
      // Convertir y descargar
      const byteCharacters = atob(boleta.archivo_pdf_base64);
      const byteNumbers = new Array(byteCharacters.length);
      for (let i = 0; i < byteCharacters.length; i++) {
        byteNumbers[i] = byteCharacters.charCodeAt(i);
      }
      const byteArray = new Uint8Array(byteNumbers);
      const blob = new Blob([byteArray], { type: 'application/pdf' });
      
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = boleta.nombre_archivo;
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  return (
    <div>
      <input
        type="text"
        value={anio}
        onChange={(e) => setAnio(e.target.value)}
        placeholder="Año (YYYY)"
      />
      <input
        type="text"
        value={mes}
        onChange={(e) => setMes(e.target.value)}
        placeholder="Mes (MM)"
      />
      <button onClick={handleDescargar} disabled={loading}>
        {loading ? 'Descargando...' : 'Descargar Boleta'}
      </button>
      {error && <div className="error">{error}</div>}
    </div>
  );
};
```

---

### 📱 React Native (con Axios)

```typescript
import axios from 'axios';
import RNFS from 'react-native-fs';
import { Alert } from 'react-native';
import FileViewer from 'react-native-file-viewer';

// Función para obtener boleta
const obtenerBoletaPago = async (
  anio: string,
  mes: string,
  accessToken: string
) => {
  try {
    const response = await axios.get(
      'https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/boleta-pago',
      {
        params: { anio, mes },
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );

    return response.data;
  } catch (error: any) {
    if (error.response?.status === 404) {
      const errorMsg = error.response.data.detail;
      Alert.alert('Boleta no encontrada', errorMsg);
      return null;
    }
    
    Alert.alert('Error', 'No se pudo obtener la boleta');
    return null;
  }
};

// Función para guardar y abrir PDF
const descargarYVisualizarPDF = async (
  base64: string,
  nombreArchivo: string
) => {
  try {
    const path = `${RNFS.DocumentDirectoryPath}/${nombreArchivo}`;
    
    // Guardar archivo desde base64
    await RNFS.writeFile(path, base64, 'base64');
    
    // Abrir con visor de PDF
    await FileViewer.open(path);
  } catch (error) {
    Alert.alert('Error', 'No se pudo abrir el archivo PDF');
  }
};

// Uso en componente
const BoletaScreen = () => {
  const [anio, setAnio] = useState('2025');
  const [mes, setMes] = useState('09');
  const accessToken = AsyncStorage.getItem('access_token');

  const handleObtenerBoleta = async () => {
    const boleta = await obtenerBoletaPago(anio, mes, accessToken);
    if (boleta) {
      await descargarYVisualizarPDF(
        boleta.archivo_pdf_base64,
        boleta.nombre_archivo
      );
    }
  };

  return (
    <View>
      <TextInput
        value={anio}
        onChangeText={setAnio}
        placeholder="Año (YYYY)"
      />
      <TextInput
        value={mes}
        onChangeText={setMes}
        placeholder="Mes (MM)"
      />
      <Button title="Ver Boleta" onPress={handleObtenerBoleta} />
    </View>
  );
};
```

---

### 🎯 Vue.js (Composition API)

```typescript
import { ref } from 'vue';
import axios from 'axios';

export const useBoletaPago = () => {
  const loading = ref(false);
  const error = ref<string | null>(null);

  const obtenerBoleta = async (
    anio: string,
    mes: string,
    accessToken: string
  ) => {
    loading.value = true;
    error.value = null;

    try {
      const response = await axios.get(
        '/api/v1/vacaciones-permisos-mobile/boleta-pago',
        {
          params: { anio, mes },
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
          }
        }
      );

      return response.data;
    } catch (err: any) {
      if (err.response?.status === 404) {
        error.value = err.response.data.detail;
        return null;
      }
      error.value = 'Error al obtener la boleta';
      return null;
    } finally {
      loading.value = false;
    }
  };

  const descargarPDF = (base64: string, nombreArchivo: string) => {
    const byteCharacters = atob(base64);
    const byteNumbers = new Array(byteCharacters.length);
    for (let i = 0; i < byteCharacters.length; i++) {
      byteNumbers[i] = byteCharacters.charCodeAt(i);
    }
    const byteArray = new Uint8Array(byteNumbers);
    const blob = new Blob([byteArray], { type: 'application/pdf' });
    
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = nombreArchivo;
    a.click();
    URL.revokeObjectURL(url);
  };

  return {
    obtenerBoleta,
    descargarPDF,
    loading,
    error
  };
};
```

---

## 🔄 Manejo de Errores Recomendado

### Códigos de Error y Acciones

| Código HTTP | error_code | Significado | Acción Recomendada |
|-------------|------------|-------------|-------------------|
| 404 | `BOLETA_NOT_FOUND` | Boleta no existe | Mostrar mensaje y sugerir verificar año/mes |
| 404 | `BOLETA_SIN_ARCHIVO` | Boleta sin PDF | Informar y sugerir contactar RRHH |
| 404 | `CERTIFICADO_CTS_NOT_FOUND` | Certificado no existe | Mostrar mensaje y sugerir verificar año |
| 404 | `CERTIFICADO_SIN_ARCHIVO` | Certificado sin PDF | Informar y sugerir contactar RRHH |
| 401 | - | Token inválido/expirado | Redirigir al login |
| 500 | `BOLETA_CONVERSION_ERROR` | Error al procesar PDF | Mostrar error y sugerir reintentar |

---

## 📝 Notas Importantes

1. **Autenticación:**
   - ✅ Siempre incluir el Access Token en el header `Authorization`
   - ✅ Manejar token expirado (401) redirigiendo al login

2. **Validación de Parámetros:**
   - ✅ Validar formato de año (YYYY) antes de enviar
   - ✅ Validar formato de mes (MM) antes de enviar
   - ✅ Mostrar mensajes de error si los formatos son incorrectos

3. **Manejo de Base64:**
   - ✅ El PDF viene codificado en base64
   - ✅ Decodificar antes de crear el blob/archivo
   - ✅ Usar el `nombre_archivo` proporcionado para guardar

4. **UX (Experiencia de Usuario):**
   - Mostrar indicador de carga mientras se obtiene el documento
   - Mostrar mensajes claros cuando no se encuentra el documento
   - Permitir reintentar en caso de error 500
   - Opciones: descargar o visualizar en el navegador/app

5. **Seguridad:**
   - ✅ Nunca exponer el Access Token en logs o consola
   - ✅ Validar que el usuario solo pueda ver sus propios documentos
   - ✅ El backend valida automáticamente el código de trabajador del token

---

## 🧪 Ejemplo de Prueba (cURL)

### Boleta de Pago

```bash
curl -X GET "https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/boleta-pago?anio=2025&mes=09" \
  -H "Authorization: Bearer tu_access_token_aqui" \
  -H "Content-Type: application/json"
```

### Certificado CTS

```bash
curl -X GET "https://tu-dominio.com/api/v1/vacaciones-permisos-mobile/certificado-cts?anio=2024" \
  -H "Authorization: Bearer tu_access_token_aqui" \
  -H "Content-Type: application/json"
```

---

## 📞 Soporte

Si tienes dudas o problemas con la implementación, contacta al equipo de backend.

**Última actualización:** Febrero 2026
