# Diccionario de Datos - Sakila Data Warehouse

## Bases de Datos

| Base de Datos | Tipo | Propósito | Tablas |
|---------------|------|-----------|--------|
| `sakila` | OLTP | Sistema transaccional origen | 16 |
| `sakila_staging` | Intermedia | Limpieza y validación | 12 |
| `sakila_dw` | OLAP | Análisis dimensional | 9 |

---

## sakila_dw - Data Mart (Modelo Estrella)

### Dimensiones

#### dim_tiempo

**Descripción:** Dimensión temporal con granularidad diaria

| Campo | Tipo | Restricciones | Descripción |
|-------|------|---------------|-------------|
| `fecha_id` | INT | PK, NOT NULL | Clave primaria (formato: YYYYMMDD) |
| `fecha` | DATE | NOT NULL, UNIQUE | Fecha calendario |
| `anio` | INT | NOT NULL | Año (2005-2026) |
| `trimestre` | INT | NOT NULL | Trimestre (1-4) |
| `mes` | INT | NOT NULL | Mes (1-12) |
| `mes_nombre` | VARCHAR(20) | | Nombre del mes (Enero, Febrero...) |
| `dia` | INT | NOT NULL | Día del mes (1-31) |
| `dia_semana` | INT | NOT NULL | Día de la semana (1=Lunes, 7=Domingo) |
| `dia_semana_nombre` | VARCHAR(20) | | Nombre del día (Lunes, Martes...) |
| `semana_anio` | INT | NOT NULL | Número de semana del año (1-53) |
| `es_fin_semana` | BOOLEAN | | TRUE si es sábado o domingo |
| `es_festivo` | BOOLEAN | DEFAULT FALSE | Indicador de día festivo |
| `fecha_inicio` | DATETIME | DEFAULT NOW() | Fecha de creación del registro |
| `fecha_fin` | DATETIME | DEFAULT '9999-12-31' | Fecha de fin de vigencia |
| `version` | INT | DEFAULT 1 | Versión del registro (SCD Type 2) |
| `activo` | BOOLEAN | DEFAULT TRUE | Registro actualmente activo |

**Índices:**
- `idx_fecha` (fecha)
- `idx_anio_mes` (anio, mes)
- `idx_activo` (activo)

**Cardinalidad:** ~8,035 registros (2005-2026)

**Ejemplo:**
```
fecha_id: 20050524
fecha: 2005-05-24
anio: 2005
trimestre: 2
mes: 5
mes_nombre: May
dia: 24
dia_semana: 3 (Miércoles)
es_fin_semana: FALSE
```

---

#### dim_film

**Descripción:** Dimensión de películas con SCD Type 2 para rastrear cambios

| Campo | Tipo | Restricciones | Descripción |
|-------|------|---------------|-------------|
| `film_sk` | INT | PK, AUTO_INCREMENT | Clave sustituta (surrogate key) |
| `film_id` | INT | NOT NULL | Clave natural de Sakila |
| `titulo` | VARCHAR(255) | NOT NULL | Título de la película |
| `descripcion` | TEXT | | Sinopsis de la película |
| `anio_lanzamiento` | INT | | Año de estreno |
| `duracion` | INT | | Duración en minutos |
| `clasificacion` | VARCHAR(10) | | Rating (G, PG, PG-13, R, NC-17) |
| `tarifa_renta` | DECIMAL(4,2) | | Precio de renta en USD |
| `costo_reemplazo` | DECIMAL(5,2) | | Costo de reposición en USD |
| `fecha_inicio` | DATETIME | DEFAULT NOW() | Inicio de vigencia |
| `fecha_fin` | DATETIME | DEFAULT '9999-12-31' | Fin de vigencia |
| `version` | INT | DEFAULT 1 | Número de versión |
| `activo` | BOOLEAN | DEFAULT TRUE | TRUE si es la versión actual |

**Índices:**
- `idx_film_id` (film_id)
- `idx_activo` (activo)
- `idx_titulo` (titulo)

**Cardinalidad:** ~1,000 registros (películas únicas)

**SCD Type 2:** Rastrea cambios en `tarifa_renta`

**Ejemplo:**
```
film_sk: 45
film_id: 12
titulo: "MATRIX"
tarifa_renta: 2.99
version: 1
activo: TRUE
```

---

#### dim_categoria

**Descripción:** Dimensión de categorías/géneros de películas

| Campo | Tipo | Restricciones | Descripción |
|-------|------|---------------|-------------|
| `categoria_sk` | INT | PK, AUTO_INCREMENT | Clave sustituta |
| `categoria_id` | INT | NOT NULL | Clave natural de Sakila |
| `nombre_categoria` | VARCHAR(50) | NOT NULL | Nombre del género |
| `descripcion` | VARCHAR(255) | | Descripción de la categoría |
| `fecha_inicio` | DATETIME | DEFAULT NOW() | Metadatos SCD |
| `fecha_fin` | DATETIME | DEFAULT '9999-12-31' | Metadatos SCD |
| `version` | INT | DEFAULT 1 | Versión del registro |
| `activo` | BOOLEAN | DEFAULT TRUE | Registro activo |

**Índices:**
- `idx_categoria_id` (categoria_id)
- `idx_activo` (activo)

**Cardinalidad:** 16 registros

**Valores:**
- Action
- Animation
- Children
- Classics
- Comedy
- Documentary
- Drama
- Family
- Foreign
- Games
- Horror
- Music
- New
- Sci-Fi
- Sports
- Travel

---

#### dim_tienda

**Descripción:** Dimensión de tiendas físicas con ubicación geográfica

| Campo | Tipo | Restricciones | Descripción |
|-------|------|---------------|-------------|
| `tienda_sk` | INT | PK, AUTO_INCREMENT | Clave sustituta |
| `tienda_id` | INT | NOT NULL | Clave natural de Sakila |
| `nombre_tienda` | VARCHAR(100) | | Nombre descriptivo |
| `direccion` | VARCHAR(100) | | Dirección física |
| `ciudad` | VARCHAR(50) | | Ciudad |
| `pais` | VARCHAR(50) | | País |
| `codigo_postal` | VARCHAR(10) | | Código postal |
| `fecha_inicio` | DATETIME | DEFAULT NOW() | Metadatos SCD |
| `fecha_fin` | DATETIME | DEFAULT '9999-12-31' | Metadatos SCD |
| `version` | INT | DEFAULT 1 | Versión del registro |
| `activo` | BOOLEAN | DEFAULT TRUE | Registro activo |

**Índices:**
- `idx_tienda_id` (tienda_id)
- `idx_activo` (activo)
- `idx_ciudad` (ciudad)
- `idx_pais` (pais)

**Cardinalidad:** 2 registros

**Ejemplo:**
```
tienda_sk: 1
tienda_id: 1
nombre_tienda: "Tienda 1"
ciudad: "Lethbridge"
pais: "Canada"
```

---

### Tabla de Hechos

#### fact_ventas

**Descripción:** Tabla de hechos con datos agregados de rentas por día/película/categoría/tienda

| Campo | Tipo | Restricciones | Descripción |
|-------|------|---------------|-------------|
| `venta_id` | BIGINT | PK, AUTO_INCREMENT | Identificador único |
| `fecha_id` | INT | FK, NOT NULL | Referencia a dim_tiempo |
| `film_sk` | INT | FK, NOT NULL | Referencia a dim_film |
| `categoria_sk` | INT | FK, NOT NULL | Referencia a dim_categoria |
| `tienda_sk` | INT | FK, NOT NULL | Referencia a dim_tienda |
| `cantidad_rentas` | INT | NOT NULL, DEFAULT 0 | Número de rentas en el período |
| `monto_total` | DECIMAL(10,2) | NOT NULL, DEFAULT 0.00 | Ingresos totales en USD |
| `monto_promedio` | DECIMAL(10,2) | | Ticket promedio en USD |
| `dias_renta_promedio` | DECIMAL(5,2) | | Duración promedio de renta en días |
| `cantidad_devoluciones` | INT | DEFAULT 0 | Rentas devueltas |
| `fecha_carga` | DATETIME | DEFAULT NOW() | Timestamp de carga ETL |
| `etl_id` | INT | | ID de ejecución ETL |

**Índices:**
- `idx_fecha` (fecha_id)
- `idx_film` (film_sk)
- `idx_categoria` (categoria_sk)
- `idx_tienda` (tienda_sk)
- `idx_fecha_tienda` (fecha_id, tienda_sk)
- `idx_fecha_categoria` (fecha_id, categoria_sk)

**Foreign Keys:**
- `fecha_id` → dim_tiempo(fecha_id)
- `film_sk` → dim_film(film_sk)
- `categoria_sk` → dim_categoria(categoria_sk)
- `tienda_sk` → dim_tienda(tienda_sk)

**Granularidad:** Fecha + Película + Categoría + Tienda

**Cardinalidad:** Variable (~miles de registros dependiendo del período)

**Métricas Calculadas:**
- `monto_promedio` = `monto_total` / `cantidad_rentas`
- `dias_renta_promedio` = AVG(return_date - rental_date)

**Ejemplo:**
```
venta_id: 1001
fecha_id: 20050524
film_sk: 45
categoria_sk: 3
tienda_sk: 1
cantidad_rentas: 3
monto_total: 8.97
monto_promedio: 2.99
dias_renta_promedio: 5.0
cantidad_devoluciones: 3
```

---

### Vistas Analíticas

#### v_top_films_categoria

**Descripción:** Top películas más rentadas por categoría

**Columnas:**
- `nombre_categoria` VARCHAR(50)
- `titulo` VARCHAR(255)
- `total_rentas` BIGINT
- `ingresos_totales` DECIMAL(18,2)
- `precio_promedio` DECIMAL(10,2)

**Ordenamiento:** Por categoría, luego por total_rentas DESC

---

#### v_ventas_mensuales_tienda

**Descripción:** Evolución mensual de ventas por tienda

**Columnas:**
- `anio` INT
- `mes` INT
- `mes_nombre` VARCHAR(20)
- `nombre_tienda` VARCHAR(100)
- `ciudad` VARCHAR(50)
- `total_rentas` BIGINT
- `ingresos_totales` DECIMAL(18,2)
- `ticket_promedio` DECIMAL(10,2)

**Ordenamiento:** Por año, mes, tienda

---

#### v_performance_categoria

**Descripción:** Métricas consolidadas por categoría

**Columnas:**
- `nombre_categoria` VARCHAR(50)
- `total_peliculas` BIGINT - Películas únicas
- `total_rentas` BIGINT
- `ingresos_totales` DECIMAL(18,2)
- `precio_promedio` DECIMAL(10,2)
- `dias_renta_promedio` DECIMAL(10,2)

**Ordenamiento:** Por ingresos_totales DESC

---

#### v_resumen_ejecutivo

**Descripción:** KPIs ejecutivos mensuales

**Columnas:**
- `anio` INT
- `mes` INT
- `peliculas_rentadas` BIGINT - Películas únicas rentadas
- `tiendas_activas` BIGINT
- `total_rentas` BIGINT
- `ingresos_totales` DECIMAL(18,2)
- `ticket_promedio` DECIMAL(10,2)
- `rentas_por_tienda` DECIMAL(10,2)

**Ordenamiento:** Por año DESC, mes DESC

---

## sakila_staging - Área Intermedia

### Tablas de Control

#### etl_control

**Descripción:** Auditoría de ejecuciones ETL

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `etl_id` | INT PK AUTO_INCREMENT | Identificador único |
| `proceso` | VARCHAR(100) | Nombre del proceso (EXTRACCION_COMPLETA, etc.) |
| `fecha_inicio` | DATETIME | Timestamp de inicio |
| `fecha_fin` | DATETIME | Timestamp de finalización |
| `estado` | ENUM | INICIADO, COMPLETADO, ERROR |
| `registros_leidos` | INT | Total de registros leídos |
| `registros_escritos` | INT | Total de registros escritos |
| `registros_error` | INT | Total de errores |
| `mensaje_error` | TEXT | Descripción de error (si aplica) |
| `duracion_segundos` | INT | Duración calculada |

**Ejemplo:**
```
etl_id: 5
proceso: "EXTRACCION_COMPLETA"
fecha_inicio: 2025-10-04 16:00:00
fecha_fin: 2025-10-04 16:00:35
estado: COMPLETADO
registros_leidos: 39999
registros_escritos: 39999
duracion_segundos: 35
```

---

#### audit_calidad

**Descripción:** Registro de validaciones de calidad

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `audit_id` | INT PK AUTO_INCREMENT | Identificador único |
| `etl_id` | INT FK | Referencia a etl_control |
| `tabla_origen` | VARCHAR(100) | Tabla validada |
| `tabla_destino` | VARCHAR(100) | Tabla comparada |
| `validacion` | VARCHAR(255) | Nombre de la validación |
| `resultado` | ENUM | PASS, FAIL, WARNING |
| `valor_esperado` | VARCHAR(100) | Valor esperado |
| `valor_obtenido` | VARCHAR(100) | Valor real |
| `mensaje` | TEXT | Detalles de la validación |
| `fecha_validacion` | DATETIME | Timestamp |

**Ejemplo:**
```
validacion: "Duplicados PK"
resultado: PASS
tabla_origen: "stg_rental"
valor_esperado: "0"
valor_obtenido: "0"
mensaje: "No se encontraron duplicados"
```

---

### Tablas Staging

Las tablas `stg_*` replican la estructura de Sakila con columnas adicionales:

**Columnas de Metadatos (todas las tablas stg_*):**
- `etl_fecha_carga` DATETIME - Timestamp de carga
- `etl_id` INT - ID de ejecución ETL

**Columnas de Validación (stg_rental, stg_payment, stg_film):**
- `es_valido` BOOLEAN - TRUE si pasó validaciones
- `mensaje_validacion` VARCHAR(255) - Razón de invalidez

---

## Reglas de Negocio

### Validaciones Implementadas

**Duplicados:**
- No duplicados en claves primarias
- Si se detectan: se elimina el más antiguo (por etl_fecha_carga)

**Valores Nulos:**
- Campos requeridos no pueden ser NULL
- rental_id, customer_id, amount, payment_date son obligatorios

**Rangos Numéricos:**
- `amount` >= 0 AND <= 100
- `rental_rate` >= 0 AND <= 10
- `length` > 0 AND <= 500

**Fechas:**
- `rental_date` no puede ser futura
- `return_date` >= `rental_date` (si existe)
- `payment_date` no puede ser futura

**Integridad Referencial:**
- Todas las FK deben existir en tabla padre
- rental.inventory_id → inventory.inventory_id
- inventory.film_id → film.film_id
- store.address_id → address.address_id

---

## Tipos de Datos

### Convenciones

| Tipo SQL | Uso | Ejemplo |
|----------|-----|---------|
| INT | IDs, contadores | cantidad_rentas |
| BIGINT | IDs grandes, autoincrement fact | venta_id |
| DECIMAL(10,2) | Montos monetarios | monto_total |
| DECIMAL(5,2) | Métricas pequeñas | dias_renta_promedio |
| VARCHAR(N) | Textos cortos | nombre_categoria |
| TEXT | Textos largos | descripcion |
| DATE | Solo fecha | fecha (dim_tiempo) |
| DATETIME | Fecha + hora | fecha_inicio, fecha_carga |
| BOOLEAN | Flags binarios | activo, es_valido |
| ENUM | Valores predefinidos | estado, resultado |

---

## Glosario de Términos

**Clave Sustituta (Surrogate Key):** Clave artificial (film_sk) vs natural (film_id)

**SCD (Slowly Changing Dimension):** Técnica para rastrear cambios históricos

**Granularidad:** Nivel de detalle de cada fila (día, hora, transacción)

**Tabla de Hechos:** Tabla con métricas cuantificables

**Dimensión:** Tabla con atributos descriptivos para análisis

**ETL:** Extract, Transform, Load

**OLTP:** Online Transaction Processing (transaccional)

**OLAP:** Online Analytical Processing (analítico)

**CDC:** Change Data Capture (captura de cambios)

**Carga Incremental:** Solo cargar datos nuevos/modificados

**Carga Completa:** Cargar todos los datos desde cero

---

## Cambios de Schema (Historial)

| Fecha | Versión | Cambio |
|-------|---------|--------|
| 2025-10-04 | 1.0 | Creación inicial del Data Mart |

---

## Contacto y Soporte

Para preguntas sobre este diccionario de datos:
- Ver README.md
- Ver ARCHITECTURE.md
- Revisar código fuente en `src/`