[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://python.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-orange.svg)](https://www.mysql.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

# Sistema ETL: Sakila OLTP → Data Mart OLAP

Proyecto académico de ETL que transforma datos de una base de datos transaccional (Sakila) a un modelo dimensional optimizado para análisis de negocio.

## Descripción del Proyecto

Este proyecto implementa un proceso ETL completo que extrae datos del sistema operacional Sakila (simulando una tienda de alquiler de películas), realiza validaciones de calidad, limpieza y transformaciones, para finalmente cargar los datos en un Data Mart con esquema estrella optimizado para consultas analíticas.

### Objetivo

Demostrar la diferencia de rendimiento entre consultas OLTP (Online Transaction Processing) sobre bases de datos normalizadas versus consultas OLAP (Online Analytical Processing) sobre modelos dimensionales desnormalizados.

## Arquitectura del Sistema

```
┌─────────────────┐
│  Sakila (OLTP)  │  ← Base de datos transaccional (normalizada 3FN)
│   MySQL Local   │
└────────┬────────┘
         │ Extracción (Python + SQLAlchemy)
         ↓
┌─────────────────┐
│ Staging Area    │  ← Área intermedia para limpieza y validación
│ sakila_staging  │
└────────┬────────┘
         │ Transformación (Pandas + SQL)
         ↓
┌─────────────────┐
│  Data Mart      │  ← Modelo Estrella (OLAP)
│  sakila_dw      │  → 4 Dimensiones + 1 Tabla de Hechos
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│   Power BI      │  ← Dashboards y análisis de negocio
└─────────────────┘
```

## Tecnologías Utilizadas

- **Python 3.12** - Lenguaje principal
- **UV** - Gestor de paquetes y entornos virtuales
- **MySQL 8.0** - Base de datos
- **Jupyter Notebook** - Desarrollo interactivo
- **Pandas** - Manipulación de datos
- **SQLAlchemy** - ORM y conexiones
- **Power BI** - Visualización (opcional)

### Librerías Python

```
pandas
numpy
sqlalchemy
pymysql
cryptography
python-dotenv
colorlog
jupyter
ipykernel
```

## Requisitos Previos

1. Python 3.11+
2. MySQL 8.0+ instalado y corriendo
3. Base de datos Sakila cargada
4. UV instalado (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Instalación

### 1. Clonar/Descargar el proyecto

```bash
cd ruta/del/proyecto
```

### 2. Instalar dependencias con UV

```bash
uv add pandas numpy sqlalchemy pymysql cryptography python-dotenv colorlog jupyter ipykernel
```

### 3. Configurar credenciales

Crear archivo `.env` en la raíz del proyecto:

```env
# Base de datos Sakila (origen)
SAKILA_HOST=localhost
SAKILA_PORT=3306
SAKILA_USER=root
SAKILA_PASSWORD=tu_password
SAKILA_DATABASE=sakila

# Data Mart (destino)
DM_HOST=localhost
DM_PORT=3306
DM_USER=root
DM_PASSWORD=tu_password
DM_DATABASE=sakila_dw

# Staging
STAGING_DATABASE=sakila_staging

# ETL Config
ETL_BATCH_SIZE=1000
ETL_LOG_LEVEL=INFO
ETL_LOG_PATH=logs/
```

### 4. Verificar configuración

```bash
uv run jupyter notebook
# Abrir: notebooks/00_test_config.ipynb
# Ejecutar todas las celdas
```

## Estructura del Proyecto

```
appBigData/
├── .env                          # Credenciales (NO subir a Git)
├── .env.example                 # Plantilla de credenciales
├── .gitignore                   # Archivos excluidos
├── README.md                    # Este archivo
├── config/
│   └── config.py                # Configuración centralizada
├── logs/
│   └── etl_YYYYMMDD.log        # Logs por fecha
├── notebooks/
│   ├── 00_test_config.ipynb    # Verificación inicial
│   ├── 01_extraccion.ipynb     # Extracción de Sakila
│   ├── 02_staging.ipynb        # Validaciones y limpieza
│   └── 03_transformacion.ipynb # Modelo estrella
├── sql/
│   ├── create_staging.sql      # Schema de staging
│   └── create_datamart.sql     # Schema Data Mart
└── src/
    ├── __init__.py
    ├── logger_config.py        # Sistema de logging
    ├── extractor.py            # Módulo de extracción
    ├── validator.py            # Validaciones de calidad
    ├── staging.py              # Procesamiento staging
    └── transformer.py          # Transformaciones DM
```

## Uso del Sistema

### Ejecución paso a paso (notebooks)

```bash
# Iniciar Jupyter
uv run jupyter notebook

# Ejecutar notebooks en orden:
# 1. notebooks/01_extraccion.ipynb
# 2. notebooks/02_staging.ipynb
# 3. notebooks/03_transformacion.ipynb
```

### Orden de ejecución

1. **Extracción** (01_extraccion.ipynb)
   - Extrae ~40,000 registros de Sakila
   - Carga a staging con metadatos ETL
   - Duración: ~30 segundos

2. **Staging y Validaciones** (02_staging.ipynb)
   - Validaciones PRE-limpieza
   - Limpieza de duplicados, nulos, normalizaciones
   - Validaciones POST-limpieza
   - Registra auditoría de calidad
   - Duración: ~20 segundos

3. **Transformación** (03_transformacion.ipynb)
   - Crea modelo estrella
   - Puebla 4 dimensiones
   - Agrega datos en tabla de hechos
   - Genera vistas analíticas
   - Duración: ~40 segundos

### Ejecución automatizada (script orquestador)

Para ejecutar todo el flujo ETL de una vez:
```bash
# Extracción completa (primera vez)
uv run python main_etl.py

# Extracción incremental (cargas posteriores)
uv run python main_etl.py --incremental

# Sin confirmación (para automatización)
uv run python main_etl.py --force

# Omitir validaciones (no recomendado)
uv run python main_etl.py --skip-validation

## Modelo de Datos

### Modelo Estrella (Star Schema)

```
                    ┌──────────────┐
                    │  dim_tiempo  │
                    │──────────────│
                    │ fecha_id PK  │
                    │ fecha        │
                    │ año          │
                    │ mes          │
                    │ trimestre    │
                    └──────┬───────┘
                           │
    ┌──────────────┐       │       ┌──────────────┐
    │  dim_film    │       │       │ dim_categoria│
    │──────────────│       │       │──────────────│
    │ film_sk PK   │       │       │categoria_sk  │
    │ film_id      │       │       │categoria_id  │
    │ titulo       │       │       │nombre        │
    │ tarifa_renta │       │       └──────┬───────┘
    └──────┬───────┘       │              │
           │               │              │
           │      ┌────────┴────────┐     │
           └──────┤  fact_ventas    ├─────┘
                  │─────────────────│
                  │ venta_id PK     │
                  │ fecha_id FK     │
                  │ film_sk FK      │
                  │ categoria_sk FK │
                  │ tienda_sk FK    │
                  │ cantidad_rentas │
                  │ monto_total     │
                  │ monto_promedio  │
                  └────────┬────────┘
                           │
                  ┌────────┴───────┐
                  │  dim_tienda    │
                  │────────────────│
                  │ tienda_sk PK   │
                  │ tienda_id      │
                  │ nombre         │
                  │ ciudad         │
                  │ pais           │
                  └────────────────┘
```

### Dimensiones

**dim_tiempo**
- 8,035 registros (2005-2026)
- Atributos: año, mes, trimestre, día_semana
- Granularidad: día

**dim_film**
- 1,000 películas
- SCD Type 2: Rastrea cambios en tarifas
- Atributos: título, duración, rating, costo

**dim_categoria**
- 16 categorías
- Atributos: nombre, descripción

**dim_tienda**
- 2 tiendas
- Atributos: ubicación completa (ciudad, país)

### Tabla de Hechos

**fact_ventas**
- Granularidad: fecha + film + categoría + tienda
- Métricas:
  - cantidad_rentas
  - monto_total
  - monto_promedio
  - dias_renta_promedio
  - cantidad_devoluciones

## Vistas Analíticas Predefinidas

El sistema incluye 4 vistas optimizadas para análisis de negocio:

### 1. v_top_films_categoria
Top películas más rentadas por categoría
```sql
SELECT * FROM v_top_films_categoria;
```

### 2. v_ventas_mensuales_tienda
Evolución mensual de ventas por tienda
```sql
SELECT * FROM v_ventas_mensuales_tienda 
WHERE anio = 2005;
```

### 3. v_performance_categoria
Performance consolidado por categoría
```sql
SELECT * FROM v_performance_categoria 
ORDER BY ingresos_totales DESC;
```

### 4. v_resumen_ejecutivo
KPIs ejecutivos mensuales
```sql
SELECT * FROM v_resumen_ejecutivo 
ORDER BY anio DESC, mes DESC 
LIMIT 12;
```

## Métricas de Calidad

El sistema implementa validaciones automáticas:

- Validación de duplicados en PKs
- Validación de valores nulos en campos requeridos
- Validación de rangos numéricos (montos ≥ 0)
- Integridad referencial (foreign keys válidas)
- Consistencia de totales entre tablas

Resultados registrados en: `sakila_staging.audit_calidad`

## Logs y Auditoría

### Sistema de Logging

- Logs en consola con colores
- Logs en archivo: `logs/etl_YYYYMMDD.log`
- Niveles: INFO, WARNING, ERROR

### Tabla de Control ETL

```sql
SELECT * FROM sakila_staging.etl_control 
ORDER BY etl_id DESC;
```

Registra:
- Proceso ejecutado
- Fecha/hora inicio y fin
- Registros leídos/escritos/error
- Duración en segundos
- Estado (INICIADO/COMPLETADO/ERROR)

## Comparación de Performance

### Benchmark: OLTP vs OLAP

Query: "Top 20 películas por ingresos con categoría"

**OLTP (Sakila normalizado)**
- 6 JOINs necesarios
- Tiempo promedio: ~0.15 segundos

**OLAP (Data Mart estrella)**
- 3 JOINs (dimensiones pre-agregadas)
- Tiempo promedio: ~0.05 segundos

**Mejora: 3x más rápido**

## Características Avanzadas

### SCD Type 2 (Slowly Changing Dimensions)

Implementado en `dim_film` para rastrear cambios históricos:

- Registra versiones anteriores de películas
- Mantiene historial de tarifas
- Campos de control: `fecha_inicio`, `fecha_fin`, `activo`, `version`

Ejemplo:
```sql
-- Ver historial de una película
SELECT * FROM dim_film 
WHERE film_id = 1 
ORDER BY version;
```

### Extracción Incremental

El sistema soporta cargas incrementales:

```python
from src.extractor import SakilaExtractor

extractor = SakilaExtractor()
# Extrae solo registros nuevos desde última carga
stats = extractor.extraer_todas_las_tablas(incremental=True)
```

## Troubleshooting

### Error: Access denied for user 'root'@'localhost'

Verificar credenciales en `.env` y permisos de usuario MySQL:
```sql
GRANT ALL PRIVILEGES ON sakila.* TO 'root'@'localhost';
GRANT ALL PRIVILEGES ON sakila_staging.* TO 'root'@'localhost';
GRANT ALL PRIVILEGES ON sakila_dw.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
```

### Error: cryptography package is required

```bash
uv add cryptography
```

### Error: Column 'es_valido' doesn't exist

Ejecutar celda de preparación en notebook 02 que agrega columnas de validación.

### Problema: Foreign key constraint en DROP TABLE

Las dimensiones deben limpiarse con `TRUNCATE` en lugar de `DROP`:
```sql
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE dim_tiempo;
SET FOREIGN_KEY_CHECKS = 1;
```

## Limitaciones Conocidas

1. Campo `location` (GEOMETRY) de tabla `address` excluido por incompatibilidad
2. Sistema optimizado para cargas batch, no streaming
3. SCD Type 2 solo implementado en `dim_film` como ejemplo

## Mejoras Futuras

- Implementar Apache Airflow para orquestación
- Agregar soporte para múltiples fuentes (web, APIs)
- Implementar particionamiento de fact_ventas por fecha
- Agregar índices columnares para queries más complejas
- Dashboard interactivo con Streamlit
- Tests unitarios para módulos ETL

## Requisitos Funcionales Implementados

- RF1: Extracción de datos de Sakila
- RF2: Staging / área intermedia
- RF3-4: Transformaciones analíticas / Data Mart
- RF5: Consultas analíticas / reporting
- RF6: Integración con Power BI (preparado)
- RF7: Auditoría / logging
- RF8: Validaciones de calidad de datos
- RF9: (Implícito) Manejo de errores
- RF10: Orquestación del proceso

## Requisitos No Funcionales Cumplidos

- Performance: ETL completo < 2 minutos
- Escalabilidad: Arquitectura modular extensible
- Confiabilidad: Manejo de errores y reintentos
- Seguridad: Credenciales en .env, no en código
- Mantenibilidad: Código modular y documentado
- Documentación: README, docstrings, comentarios

## Autor
Leodan Merino Daza
Proyecto académico - SENATI Octavo Ciclo
Curso: Big Data y Análisis de Datos

## Licencia

Proyecto educativo - Uso académico