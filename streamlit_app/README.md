# 🎬 Sakila Dashboard - Streamlit

## 📋 Descripción General

Dashboard interactivo desarrollado con Streamlit para el análisis del Data Warehouse de Sakila. Proporciona visualizaciones en tiempo real de ventas, películas, categorías y tiendas con capacidad de filtrado y exportación de datos.

---

## 🚀 Características Principales

### **Página Principal (Home)**
- KPIs principales del negocio
- Resumen ejecutivo de ventas
- Top películas más rentadas
- Análisis por categorías
- Comparativa de tiendas
- Visualizaciones interactivas con Plotly

### **Página de Ventas** 📊
- Evolución temporal de ventas
- Análisis por tienda
- Distribución por categoría
- Exportación de datos en CSV
- Filtros dinámicos por fecha, tienda y categoría

### **Página de Películas** 🎬
- Top películas por rentas y revenue
- Análisis por clasificación (G, PG, PG-13, R, NC-17)
- Correlación duración vs popularidad
- Búsqueda avanzada de películas
- Análisis de tarifas vs revenue

### **Página de Categorías** 📂
- Ranking de géneros cinematográficos
- Tendencias temporales por categoría
- Top películas por género
- Análisis de estacionalidad
- Performance detallado

### **Página de Tiendas** 🏪
- Comparativa entre tiendas
- Vista individual por tienda
- Evolución temporal
- Análisis de brechas y diferencias
- Gráficos radar comparativos

---

## 🏗️ Arquitectura del Dashboard

```
streamlit_app/
├── app.py                          # Punto de entrada principal
├── pages/                          # Páginas secundarias
│   ├── 1_Ventas.py
│   ├── 2_Películas.py
│   ├── 3_Categorías.py
│   └── 4_Tiendas.py
├── components/                     # Componentes reutilizables
│   ├── kpi_cards.py               # Tarjetas de métricas
│   ├── filters.py                 # Filtros interactivos
│   └── charts.py                  # Funciones de gráficos
└── utils/                         # Utilidades
    ├── db_connection.py           # Conexión a base de datos
    └── queries.py                 # Queries SQL predefinidas
```

---

## 🔧 Instalación y Configuración

### **1. Prerrequisitos**
```bash
# Asegúrate de tener instalado:
- Python 3.12+
- MySQL 8.0+
- UV (gestor de paquetes)
- Base de datos sakila_dw poblada
```

### **2. Instalar Dependencias**
```bash
# Desde la raíz del proyecto
uv add streamlit plotly
```

### **3. Configuración**
El dashboard utiliza la configuración existente en `config/config.py` y el archivo `.env`:

```env
# Variables necesarias (ya configuradas)
DM_HOST=localhost
DM_PORT=3306
DM_USER=tu_usuario
DM_PASSWORD=tu_password
DM_DATABASE=sakila_dw
```

### **4. Configuración de Streamlit** (Opcional)
Edita `.streamlit/config.toml` para personalizar el tema:

```toml
[theme]
primaryColor = "#FF4B4B"
backgroundColor = "#0E1117"
secondaryBackgroundColor = "#262730"
textColor = "#FAFAFA"
font = "sans serif"
```

---

## 🚀 Ejecución

### **Método 1: Con UV (Recomendado)**
```bash
uv run streamlit run streamlit_app/app.py
```

### **Método 2: Con entorno activado**
```bash
# Activar entorno virtual
.venv\Scripts\activate

# Ejecutar dashboard
streamlit run streamlit_app/app.py
```

### **Método 3: Script de acceso rápido**
Crea un archivo `run_dashboard.bat` en la raíz:

```batch
@echo off
echo 🚀 Iniciando Sakila Dashboard...
uv run streamlit run streamlit_app/app.py
```

Ejecuta: `.\run_dashboard.bat`

---

## 📊 Uso del Dashboard

### **Navegación**
- **Sidebar izquierdo**: Navegación entre páginas y filtros globales
- **Tabs horizontales**: Diferentes vistas dentro de cada página
- **Filtros dinámicos**: Actualización en tiempo real de visualizaciones

### **Filtros Disponibles**
- **Fecha**: Rango de fechas personalizado
- **Categorías**: Selección múltiple de géneros
- **Tiendas**: Filtro por ubicación
- **Clasificación**: Ratings de películas (G, PG, PG-13, R, NC-17)
- **Búsqueda**: Texto libre para películas

### **Exportación de Datos**
Cada página incluye botones para descargar datos en formato CSV:
- 📥 Ventas mensuales
- 📥 Análisis de películas
- 📥 Performance por categoría
- 📥 Datos de tiendas

---

## 🎨 Tipos de Visualizaciones

### **Gráficos de Líneas**
- Tendencias temporales
- Evolución de métricas
- Comparativas entre entidades

### **Gráficos de Barras**
- Rankings y top N
- Comparativas categóricas
- Distribuciones

### **Gráficos de Pastel/Dona**
- Distribución porcentual
- Composición de categorías

### **Gráficos de Dispersión**
- Correlaciones
- Análisis multivariable
- Identificación de patrones

### **Gráficos Radar**
- Comparativas multidimensionales
- Perfiles de tiendas

### **Tarjetas KPI**
- Métricas destacadas
- Cambios y deltas
- Indicadores clave

---

## 🔄 Actualización de Datos

### **Cache de Datos**
El dashboard utiliza caching inteligente:
- **TTL 5 minutos**: Queries SQL generales
- **TTL 10 minutos**: Tablas completas
- **Cache de recursos**: Conexiones a base de datos

### **Refrescar Manualmente**
Usa el botón **🔄 Refrescar Datos** en el sidebar para limpiar el cache y recargar.

### **Actualización Automática**
Los datos se actualizan automáticamente cuando:
1. Se ejecuta el proceso ETL (`main_etl.py`)
2. Se reinicia el dashboard
3. Expira el tiempo de cache

---

## 📈 Métricas y KPIs

### **KPIs Principales**
- **Total Ventas**: Suma de ingresos generados
- **Total Rentas**: Número de transacciones
- **Ticket Promedio**: Revenue / Número de rentas
- **Días Promedio**: Duración promedio de renta
- **Películas Rentadas**: Títulos únicos rentados

### **Métricas por Entidad**

#### **Películas**
- Total rentas por película
- Revenue generado
- Clasificación
- Duración
- Tarifa de renta

#### **Categorías**
- Total películas por género
- Rentas acumuladas
- Revenue total
- Precio promedio
- Días de renta promedio

#### **Tiendas**
- Rentas por tienda
- Ventas totales
- Ticket promedio
- Performance comparativo

---

## 🔍 Queries Utilizadas

El dashboard utiliza queries optimizadas definidas en `utils/queries.py`:

### **Vistas Analíticas**
```sql
- v_ventas_mensuales_tienda
- v_performance_categoria
- v_resumen_ejecutivo
- v_top_films_categoria
```

### **Tablas Principales**
```sql
- fact_ventas
- dim_tiempo
- dim_film
- dim_categoria
- dim_tienda
```

### **Performance**
- Todas las queries están indexadas
- Cache de resultados implementado
- Lazy loading de datos pesados

---

## 🎨 Personalización

### **Cambiar Colores**
Edita `.streamlit/config.toml`:
```toml
primaryColor = "#TU_COLOR"        # Color principal
backgroundColor = "#TU_COLOR"     # Fondo
secondaryBackgroundColor = "#TU_COLOR"  # Fondo secundario
```

### **Modificar Gráficos**
Edita `components/charts.py` para personalizar:
- Paletas de colores
- Tipos de gráficos
- Layouts predeterminados

### **Agregar Nuevas Páginas**
1. Crea archivo en `pages/` con formato: `N_Nombre.py`
2. Usa las utilidades de `components/` y `utils/`
3. Mantén la estructura consistente

### **Agregar Nuevas Queries**
Edita `utils/queries.py`:
```python
class Queries:
    MI_NUEVA_QUERY = """
        SELECT ...
        FROM ...
    """
```

---

## 🐛 Troubleshooting

### **Error: "Config object has no attribute..."**
**Solución**: Verifica que `config/config.py` tenga los métodos necesarios.

### **Error: "Unknown column in order clause"**
**Solución**: Verifica nombres de columnas en vistas SQL vs queries.py

### **Dashboard muy lento**
**Soluciones**:
- Aumenta el TTL del cache
- Reduce el número de registros mostrados
- Optimiza queries SQL con índices

### **No se muestran datos**
**Verificar**:
1. ETL ejecutado correctamente
2. Conexión a sakila_dw exitosa
3. Tablas pobladas con datos
4. Credenciales en .env correctas

### **Error de importación de módulos**
**Solución**:
```bash
# Reinstalar dependencias
uv sync
uv add streamlit plotly
```

---

## 📊 Ejemplos de Uso

### **Análisis de Tendencias**
1. Ir a página **Ventas**
2. Seleccionar rango de fechas
3. Observar evolución temporal
4. Comparar tiendas

### **Identificar Películas Populares**
1. Ir a página **Películas**
2. Aplicar filtros de clasificación
3. Ver top películas
4. Analizar correlaciones

### **Comparar Tiendas**
1. Ir a página **Tiendas**
2. Seleccionar vista **Comparativa**
3. Analizar métricas clave
4. Revisar gráfico radar

### **Exportar Reportes**
1. Navegar a cualquier página
2. Aplicar filtros deseados
3. Clic en **📥 Descargar CSV**
4. Abrir en Excel/Google Sheets

---

## 🔐 Seguridad

### **Conexiones**
- Credenciales almacenadas en `.env` (no versionado)
- Conexiones mediante SQLAlchemy con pooling
- Sin exposición de datos sensibles

### **Recomendaciones**
- No exponer el dashboard públicamente sin autenticación
- Usar variables de entorno para producción
- Implementar HTTPS si se despliega en servidor

---

## 🚀 Deployment (Opcional)

### **Streamlit Cloud (Gratis)**
```bash
# 1. Sube tu código a GitHub
# 2. Conecta en streamlit.io
# 3. Configura secrets con variables de .env
# 4. Deploy automático
```

### **Docker**
```dockerfile
FROM python:3.12
WORKDIR /app
COPY . .
RUN pip install uv
RUN uv sync
CMD ["uv", "run", "streamlit", "run", "streamlit_app/app.py"]
```

### **Servidor Local**
```bash
# Ejecutar en background
nohup uv run streamlit run streamlit_app/app.py &
```

---

## 📞 Soporte

### **Documentación Relacionada**
- `README.md` - Guía general del proyecto
- `ARCHITECTURE.md` - Arquitectura del sistema ETL
- `DATA_DICTIONARY.md` - Diccionario de datos

### **Logs**
Los logs del dashboard se encuentran en:
- Console output de Streamlit
- Logs del sistema en `/logs/`

### **Recursos Externos**
- [Documentación Streamlit](https://docs.streamlit.io)
- [Plotly Python](https://plotly.com/python/)
- [SQLAlchemy](https://docs.sqlalchemy.org)

---

## 📝 Changelog

### **v1.0.0** (2025-01-XX)
- ✅ Dashboard completo con 5 páginas
- ✅ 15+ tipos de visualizaciones
- ✅ Filtros dinámicos
- ✅ Exportación CSV
- ✅ Cache inteligente
- ✅ Responsive design
- ✅ Documentación completa

---

## 👨‍💻 Desarrollo

### **Stack Tecnológico**
- **Frontend**: Streamlit 1.50+
- **Visualización**: Plotly 6.3+
- **Base de Datos**: MySQL 8.0 via SQLAlchemy
- **Python**: 3.12+
- **Gestor de Paquetes**: UV

### **Estructura del Código**
- **Modular**: Componentes reutilizables
- **DRY**: Sin duplicación de código
- **Documentado**: Docstrings en todas las funciones
- **Tipado**: Type hints donde es posible

### **Próximas Mejoras**
- [ ] Autenticación de usuarios
- [ ] Roles y permisos
- [ ] Predicciones con ML
- [ ] Alertas y notificaciones
- [ ] Modo oscuro/claro
- [ ] Soporte multi-idioma

---

## 📄 Licencia

Este dashboard es parte del proyecto académico Sakila ETL Data Warehouse.

---

**🎬 ¡Disfruta analizando datos con Sakila Dashboard!**



