# Arquitectura del Sistema ETL - Sakila Data Warehouse

## Visión General

Sistema ETL que implementa una arquitectura de Data Warehouse moderna, transformando datos transaccionales (OLTP) en un modelo dimensional optimizado para análisis (OLAP).

## Arquitectura de Capas

```
┌────────────────────────────────────────────────────────────────┐
│                     CAPA DE PRESENTACIÓN                        │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │  Power BI    │  │   Tableau    │  │    Excel     │        │
│  │  Dashboards  │  │   Reports    │  │   Pivots     │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
└─────────────────────────────┬──────────────────────────────────┘
                              │
                              │ SQL Queries
                              ↓
┌────────────────────────────────────────────────────────────────┐
│                   CAPA ANALÍTICA (OLAP)                        │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              Data Mart: sakila_dw                        │  │
│  │                                                           │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │
│  │  │ dim_tiempo  │  │  dim_film   │  │ dim_categoria│    │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │  │
│  │         │                │                 │            │  │
│  │         └────────┬───────┴────────┬────────┘            │  │
│  │                  │                │                     │  │
│  │          ┌───────┴────────────────┴───────┐            │  │
│  │          │      fact_ventas               │            │  │
│  │          │  (Datos agregados)             │            │  │
│  │          └───────┬────────────────────────┘            │  │
│  │                  │                                      │  │
│  │         ┌────────┴──────┐                              │  │
│  │         │  dim_tienda   │                              │  │
│  │         └───────────────┘                              │  │
│  │                                                          │  │
│  │  Vistas Analíticas:                                     │  │
│  │  • v_top_films_categoria                               │  │
│  │  • v_ventas_mensuales_tienda                           │  │
│  │  • v_performance_categoria                             │  │
│  │  • v_resumen_ejecutivo                                 │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────┬──────────────────────────────────┘
                              │
                              │ ETL (Python)
                              ↓
┌────────────────────────────────────────────────────────────────┐
│                  CAPA DE TRANSFORMACIÓN                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │         Staging Area: sakila_staging                     │  │
│  │                                                           │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │  │
│  │  │stg_rental  │  │stg_payment │  │  stg_film  │        │  │
│  │  └────────────┘  └────────────┘  └────────────┘        │  │
│  │                                                           │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │  │
│  │  │stg_category│  │ stg_store  │  │  stg_city  │        │  │
│  │  └────────────┘  └────────────┘  └────────────┘        │  │
│  │                                                           │  │
│  │  Tablas de Control:                                      │  │
│  │  • etl_control (auditoría de ejecuciones)               │  │
│  │  • audit_calidad (validaciones)                         │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────┬──────────────────────────────────┘
                              │
                              │ Extracción
                              ↓
┌────────────────────────────────────────────────────────────────┐
│                   CAPA TRANSACCIONAL (OLTP)                    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │          Base de Datos: sakila                           │  │
│  │          (Modelo Normalizado - 3FN)                      │  │
│  │                                                           │  │
│  │  16 tablas relacionadas:                                 │  │
│  │  rental, payment, inventory, film, customer, store...   │  │
│  └─────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Flujo de Datos ETL

```
┌──────────────┐
│   EXTRACT    │  Extrae datos de Sakila (OLTP)
│              │  • Extracción completa o incremental
│  extractor.py│  • Detección de cambios (CDC)
│              │  • Manejo de errores
└──────┬───────┘
       │
       │ ~40,000 registros
       ↓
┌──────────────┐
│  TRANSFORM   │  Limpieza y validaciones
│              │  • Validación de calidad
│  staging.py  │  • Eliminación de duplicados
│  validator.py│  • Normalización de texto
│              │  • Marcado de inválidos
└──────┬───────┘
       │
       │ Datos limpios
       ↓
┌──────────────┐
│    LOAD      │  Carga a modelo estrella
│              │  • Población de dimensiones
│transformer.py│  • SCD Type 2 (dim_film)
│              │  • Agregación de hechos
│              │  • Creación de vistas
└──────────────┘
```

## Modelo Dimensional Detallado

### Esquema Estrella (Star Schema)

```
                          DIMENSIONES
                              
    ┌────────────────────────────────────────────────┐
    │           dim_tiempo (8,035 registros)         │
    │ ──────────────────────────────────────────────│
    │ PK: fecha_id INT                               │
    │     fecha DATE                                 │
    │     anio INT                                   │
    │     trimestre INT (1-4)                        │
    │     mes INT (1-12)                             │
    │     mes_nombre VARCHAR(20)                     │
    │     dia INT (1-31)                             │
    │     dia_semana INT (1-7)                       │
    │     dia_semana_nombre VARCHAR(20)              │
    │     semana_anio INT                            │
    │     es_fin_semana BOOLEAN                      │
    │ ──────────────────────────────────────────────│
    │ SCD: Type 1 (No cambia)                        │
    └────────────────────┬───────────────────────────┘
                         │
                         │
┌────────────────────────┼────────────────────────┐
│                        │                        │
│  dim_film (1,000)      │      dim_categoria (16)│
│ ─────────────────      │      ─────────────────│
│ PK: film_sk            │      PK: categoria_sk  │
│     film_id (NK)       │          categoria_id  │
│     titulo             │          nombre        │
│     descripcion        │                        │
│     anio_lanzamiento   │      SCD: Type 1      │
│     duracion           │                        │
│     clasificacion      │                        │
│     tarifa_renta       │                        │
│     costo_reemplazo    │                        │
│ ─────────────────      │                        │
│ SCD: Type 2            │                        │
│ • fecha_inicio         │                        │
│ • fecha_fin            │                        │
│ • version              │                        │
│ • activo               │                        │
└────────┬───────────────┴────────┬───────────────┘
         │                        │
         └────────────┬───────────┘
                      │
              ┌───────┴────────┐
              │  fact_ventas   │  TABLA DE HECHOS
              │ ───────────────│
              │ PK: venta_id   │
              │                │
              │ FK: fecha_id   │
              │ FK: film_sk    │
              │ FK: categoria_sk│
              │ FK: tienda_sk  │
              │                │
              │ MÉTRICAS:      │
              │ • cantidad_rentas    INT      │
              │ • monto_total        DECIMAL  │
              │ • monto_promedio     DECIMAL  │
              │ • dias_renta_promedio DECIMAL │
              │ • cantidad_devoluciones INT   │
              │                │
              │ METADATA:      │
              │ • fecha_carga  │
              │ • etl_id       │
              └───────┬────────┘
                      │
        ┌─────────────┴──────────────┐
        │     dim_tienda (2)         │
        │ ──────────────────────────│
        │ PK: tienda_sk              │
        │     tienda_id (NK)         │
        │     nombre_tienda          │
        │     direccion              │
        │     ciudad                 │
        │     pais                   │
        │     codigo_postal          │
        │ ──────────────────────────│
        │ SCD: Type 2                │
        └────────────────────────────┘
```

### Cardinalidades

```
dim_tiempo (1) ──────< (*) fact_ventas
dim_film (1) ────────< (*) fact_ventas
dim_categoria (1) ───< (*) fact_ventas
dim_tienda (1) ──────< (*) fact_ventas
```

### Granularidad de fact_ventas

**Nivel de detalle:** Fecha + Film + Categoría + Tienda

Cada fila representa las ventas agregadas de una película específica, en una categoría, en una tienda, en una fecha determinada.

**Ejemplo:**
```
fecha_id: 20050524
film_sk: 45
categoria_sk: 3 (Action)
tienda_sk: 1
cantidad_rentas: 3
monto_total: 8.97
```

Significa: "El 24 de mayo de 2005, la película #45 de categoría Action tuvo 3 rentas en la tienda 1, generando $8.97"

## SCD Type 2 - Slowly Changing Dimensions

### Implementación en dim_film

```
Escenario: La tarifa de renta de "Matrix" cambia de $2.99 a $3.99

┌─────────┬─────────┬─────────┬────────────┬────────────┬─────────┬────────┐
│film_sk  │film_id  │ titulo  │tarifa_renta│fecha_inicio│fecha_fin│ activo │
├─────────┼─────────┼─────────┼────────────┼────────────┼─────────┼────────┤
│   45    │   12    │ Matrix  │   2.99     │ 2024-01-01 │2024-06-30│ FALSE │ ← Histórico
│   782   │   12    │ Matrix  │   3.99     │ 2024-07-01 │9999-12-31│ TRUE  │ ← Actual
└─────────┴─────────┴─────────┴────────────┴────────────┴─────────┴────────┘

fact_ventas ahora referencia:
• Ventas de enero-junio → film_sk = 45 (tarifa $2.99)
• Ventas de julio+ → film_sk = 782 (tarifa $3.99)

Permite análisis histórico preciso:
"¿Cambiaron las ventas después del aumento de precio?"
```

### Algoritmo SCD Type 2

```python
def actualizar_dimension_scd2(registro_nuevo):
    # 1. Buscar registro activo
    existente = buscar_donde(film_id = registro_nuevo.id, activo = TRUE)
    
    # 2. Comparar atributos clave
    if existente.tarifa_renta != registro_nuevo.tarifa_renta:
        # 3. Cerrar registro anterior
        UPDATE existente
        SET fecha_fin = HOY,
            activo = FALSE
        
        # 4. Insertar nuevo registro
        INSERT nuevo_registro
        SET film_id = registro_nuevo.id,
            tarifa_renta = registro_nuevo.tarifa_renta,
            fecha_inicio = HOY,
            fecha_fin = '9999-12-31',
            version = existente.version + 1,
            activo = TRUE
```

## Arquitectura de Módulos Python

```
┌─────────────────────────────────────────┐
│         config/config.py                │
│  • Carga .env                           │
│  • Valida configuración                 │
│  • Proporciona connection strings       │
└──────────────┬──────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│      src/logger_config.py               │
│  • ETLLogger class                      │
│  • Logs a archivo + consola             │
│  • Colores por nivel                    │
│  • Métodos helper (log_etl_start, etc.) │
└──────────────┬──────────────────────────┘
               │
      ┌────────┴────────┬────────────┬─────────────┐
      ↓                 ↓            ↓             ↓
┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐
│extractor.py│  │validator.py│  │ staging.py │  │transformer │
│            │  │            │  │            │  │    .py     │
│ EXTRACT    │  │ VALIDATE   │  │ TRANSFORM  │  │   LOAD     │
│            │  │            │  │            │  │            │
│• Sakila→   │  │• Duplicados│  │• Limpieza  │  │• Dimensiones│
│  Staging   │  │• Nulos     │  │• Normalize │  │• Hechos    │
│• CDC       │  │• Rangos    │  │• Marcar    │  │• SCD Type 2│
│• Batch     │  │• FK        │  │  inválidos │  │• Vistas    │
│• Retry     │  │• Totales   │  │            │  │            │
└────────────┘  └────────────┘  └────────────┘  └────────────┘
```

### Flujo de Control

```python
# 1. Extracción
extractor = SakilaExtractor()
extractor.registrar_inicio_etl("EXTRACCION_COMPLETA")
stats = extractor.extraer_todas_las_tablas(incremental=False)
extractor.registrar_fin_etl("COMPLETADO", registros=stats)

# 2. Validación PRE
validator = DataValidator(etl_id)
resultados_pre = validator.ejecutar_validaciones_staging()

# 3. Limpieza
processor = StagingProcessor(etl_id)
stats_limpieza = processor.procesar_todas_las_tablas()

# 4. Validación POST
resultados_post = validator.ejecutar_validaciones_staging()

# 5. Transformación
transformer = DataMartTransformer(etl_id)
stats_dm = transformer.ejecutar_transformacion_completa()
```

## Patrones de Diseño Implementados

### 1. Factory Pattern
```python
def get_logger(name: str) -> logging.Logger:
    """Factory para crear loggers configurados"""
    etl_logger = ETLLogger(name, log_dir, level)
    return etl_logger.get_logger()
```

### 2. Template Method Pattern
```python
class StagingProcessor:
    def procesar_todas_las_tablas(self):
        # Template method
        self.log_start()
        for tabla in tablas:
            self.procesar_tabla(tabla)  # Hook method
        self.log_end()
    
    def procesar_tabla(self, tabla):
        # Implementación específica por tabla
        pass
```

### 3. Strategy Pattern
```python
def poblar_dim_film():
    if cambio_detectado:
        estrategia = SCD_Type_2()
    else:
        estrategia = SCD_Type_1()
    
    estrategia.actualizar_dimension(datos)
```

## Optimizaciones de Performance

### Índices en fact_ventas

```sql
CREATE INDEX idx_fecha ON fact_ventas(fecha_id);
CREATE INDEX idx_film ON fact_ventas(film_sk);
CREATE INDEX idx_categoria ON fact_ventas(categoria_sk);
CREATE INDEX idx_tienda ON fact_ventas(tienda_sk);

-- Índices compuestos para queries frecuentes
CREATE INDEX idx_fecha_tienda ON fact_ventas(fecha_id, tienda_sk);
CREATE INDEX idx_fecha_categoria ON fact_ventas(fecha_id, categoria_sk);
```

### Particionamiento (Futuro)

```sql
-- Particionar fact_ventas por año
ALTER TABLE fact_ventas
PARTITION BY RANGE (fecha_id) (
    PARTITION p2005 VALUES LESS THAN (20060101),
    PARTITION p2006 VALUES LESS THAN (20070101),
    ...
);
```

### Materialización de Vistas

```sql
-- Para queries muy frecuentes
CREATE TABLE mv_ventas_mensuales AS
SELECT * FROM v_ventas_mensuales_tienda;

-- Refresh periódico
TRUNCATE TABLE mv_ventas_mensuales;
INSERT INTO mv_ventas_mensuales 
SELECT * FROM v_ventas_mensuales_tienda;
```

## Seguridad

### Gestión de Credenciales

```
✅ Correcto:
.env (Git ignored)
  ↓
config.py (Lee .env)
  ↓
Módulos ETL

❌ Incorrecto:
Hardcoded passwords en código
```

### Permisos MySQL

```sql
-- Usuario con privilegios mínimos
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'password';

-- Solo lo necesario
GRANT SELECT ON sakila.* TO 'etl_user'@'localhost';
GRANT ALL ON sakila_staging.* TO 'etl_user'@'localhost';
GRANT ALL ON sakila_dw.* TO 'etl_user'@'localhost';
```

## Monitoreo y Alertas

### Tabla etl_control

```sql
-- Detectar ETLs fallidos
SELECT * FROM etl_control 
WHERE estado = 'ERROR' 
AND fecha_inicio >= DATE_SUB(NOW(), INTERVAL 7 DAY);

-- Monitorear duración
SELECT proceso, AVG(duracion_segundos) as promedio
FROM etl_control
WHERE estado = 'COMPLETADO'
GROUP BY proceso;
```

### Métricas Clave (KPIs del ETL)

- **Tasa de éxito**: % de ejecuciones completadas
- **Tiempo de ejecución**: Duración promedio por fase
- **Volumen de datos**: Registros procesados
- **Calidad de datos**: % validaciones pasadas
- **Disponibilidad**: Uptime del Data Mart

## Escalabilidad

### Estrategias de Crecimiento

**Vertical (Actual)**
- MySQL local
- Procesamiento en memoria con Pandas
- Apropiado para: ~1M registros

**Horizontal (Futuro)**
- Apache Spark para procesamiento distribuido
- Data Lake (S3/HDFS) como staging
- Redshift/Snowflake como Data Warehouse
- Apropiado para: >10M registros

## Conclusión

Esta arquitectura proporciona:

✅ Separación clara de capas (OLTP, Staging, OLAP)  
✅ Modelo dimensional optimizado para análisis  
✅ Trazabilidad completa (logs + auditoría)  
✅ Calidad de datos garantizada (validaciones)  
✅ Extensibilidad (agregar nuevas fuentes/dimensiones)  
✅ Performance superior (3x vs OLTP en queries analíticas)  

**Resultado:** Sistema ETL robusto, mantenible y escalable para análisis de negocio.