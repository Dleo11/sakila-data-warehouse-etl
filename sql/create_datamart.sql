-- ============================================
-- DATA MART - Modelo Estrella (Star Schema)
-- RF3-4: Transformaciones y Data Mart
-- ============================================

USE sakila_dw;

-- ============================================
-- DIMENSION: TIEMPO
-- ============================================
DROP TABLE IF EXISTS dim_tiempo;
CREATE TABLE dim_tiempo (
    fecha_id INT PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    anio INT NOT NULL,
    trimestre INT NOT NULL,
    mes INT NOT NULL,
    mes_nombre VARCHAR(20),
    dia INT NOT NULL,
    dia_semana INT NOT NULL,
    dia_semana_nombre VARCHAR(20),
    semana_anio INT NOT NULL,
    -- Flags útiles
    es_fin_semana BOOLEAN,
    es_festivo BOOLEAN DEFAULT FALSE,
    -- Metadata SCD2
    fecha_inicio DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_fin DATETIME DEFAULT '9999-12-31 23:59:59',
    version INT DEFAULT 1,
    activo BOOLEAN DEFAULT TRUE,
    INDEX idx_fecha (fecha),
    INDEX idx_anio_mes (anio, mes),
    INDEX idx_activo (activo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- DIMENSION: FILM (Película)
-- ============================================
DROP TABLE IF EXISTS dim_film;
CREATE TABLE dim_film (
    film_sk INT AUTO_INCREMENT PRIMARY KEY,
    film_id INT NOT NULL,
    titulo VARCHAR(255) NOT NULL,
    descripcion TEXT,
    anio_lanzamiento INT,
    duracion INT,
    clasificacion VARCHAR(10),
    tarifa_renta DECIMAL(4,2),
    costo_reemplazo DECIMAL(5,2),
    -- Metadata SCD2
    fecha_inicio DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_fin DATETIME DEFAULT '9999-12-31 23:59:59',
    version INT DEFAULT 1,
    activo BOOLEAN DEFAULT TRUE,
    INDEX idx_film_id (film_id),
    INDEX idx_activo (activo),
    INDEX idx_titulo (titulo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- DIMENSION: CATEGORIA
-- ============================================
DROP TABLE IF EXISTS dim_categoria;
CREATE TABLE dim_categoria (
    categoria_sk INT AUTO_INCREMENT PRIMARY KEY,
    categoria_id INT NOT NULL,
    nombre_categoria VARCHAR(50) NOT NULL,
    descripcion VARCHAR(255),
    -- Metadata SCD2
    fecha_inicio DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_fin DATETIME DEFAULT '9999-12-31 23:59:59',
    version INT DEFAULT 1,
    activo BOOLEAN DEFAULT TRUE,
    INDEX idx_categoria_id (categoria_id),
    INDEX idx_activo (activo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- DIMENSION: TIENDA
-- ============================================
DROP TABLE IF EXISTS dim_tienda;
CREATE TABLE dim_tienda (
    tienda_sk INT AUTO_INCREMENT PRIMARY KEY,
    tienda_id INT NOT NULL,
    nombre_tienda VARCHAR(100),
    direccion VARCHAR(100),
    ciudad VARCHAR(50),
    pais VARCHAR(50),
    codigo_postal VARCHAR(10),
    -- Metadata SCD2
    fecha_inicio DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_fin DATETIME DEFAULT '9999-12-31 23:59:59',
    version INT DEFAULT 1,
    activo BOOLEAN DEFAULT TRUE,
    INDEX idx_tienda_id (tienda_id),
    INDEX idx_activo (activo),
    INDEX idx_ciudad (ciudad),
    INDEX idx_pais (pais)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- TABLA DE HECHOS: VENTAS (Rentas)
-- ============================================
DROP TABLE IF EXISTS fact_ventas;
CREATE TABLE fact_ventas (
    venta_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- Foreign Keys a dimensiones
    fecha_id INT NOT NULL,
    film_sk INT NOT NULL,
    categoria_sk INT NOT NULL,
    tienda_sk INT NOT NULL,
    -- Métricas
    cantidad_rentas INT NOT NULL DEFAULT 0,
    monto_total DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    monto_promedio DECIMAL(10,2),
    dias_renta_promedio DECIMAL(5,2),
    cantidad_devoluciones INT DEFAULT 0,
    -- Metadata
    fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    -- Índices para optimizar queries analíticas
    INDEX idx_fecha (fecha_id),
    INDEX idx_film (film_sk),
    INDEX idx_categoria (categoria_sk),
    INDEX idx_tienda (tienda_sk),
    INDEX idx_fecha_tienda (fecha_id, tienda_sk),
    INDEX idx_fecha_categoria (fecha_id, categoria_sk),
    -- Foreign Keys
    FOREIGN KEY (fecha_id) REFERENCES dim_tiempo(fecha_id),
    FOREIGN KEY (film_sk) REFERENCES dim_film(film_sk),
    FOREIGN KEY (categoria_sk) REFERENCES dim_categoria(categoria_sk),
    FOREIGN KEY (tienda_sk) REFERENCES dim_tienda(tienda_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- VISTAS ANALÍTICAS
-- ============================================

-- Vista: Top películas por categoría
DROP VIEW IF EXISTS v_top_films_categoria;
CREATE VIEW v_top_films_categoria AS
SELECT 
    c.nombre_categoria,
    f.titulo,
    SUM(fv.cantidad_rentas) as total_rentas,
    SUM(fv.monto_total) as ingresos_totales,
    AVG(fv.monto_promedio) as precio_promedio
FROM fact_ventas fv
JOIN dim_film f ON fv.film_sk = f.film_sk AND f.activo = TRUE
JOIN dim_categoria c ON fv.categoria_sk = c.categoria_sk AND c.activo = TRUE
GROUP BY c.nombre_categoria, f.titulo
ORDER BY c.nombre_categoria, total_rentas DESC;

-- Vista: Ventas mensuales por tienda
DROP VIEW IF EXISTS v_ventas_mensuales_tienda;
CREATE VIEW v_ventas_mensuales_tienda AS
SELECT 
    t.anio,
    t.mes,
    t.mes_nombre,
    ti.nombre_tienda,
    ti.ciudad,
    SUM(fv.cantidad_rentas) as total_rentas,
    SUM(fv.monto_total) as ingresos_totales,
    AVG(fv.monto_promedio) as ticket_promedio
FROM fact_ventas fv
JOIN dim_tiempo t ON fv.fecha_id = t.fecha_id
JOIN dim_tienda ti ON fv.tienda_sk = ti.tienda_sk AND ti.activo = TRUE
GROUP BY t.anio, t.mes, t.mes_nombre, ti.nombre_tienda, ti.ciudad
ORDER BY t.anio, t.mes, ti.nombre_tienda;

-- Vista: Performance por categoría
DROP VIEW IF EXISTS v_performance_categoria;
CREATE VIEW v_performance_categoria AS
SELECT 
    c.nombre_categoria,
    COUNT(DISTINCT f.film_sk) as total_peliculas,
    SUM(fv.cantidad_rentas) as total_rentas,
    SUM(fv.monto_total) as ingresos_totales,
    AVG(fv.monto_promedio) as precio_promedio,
    AVG(fv.dias_renta_promedio) as dias_renta_promedio
FROM fact_ventas fv
JOIN dim_categoria c ON fv.categoria_sk = c.categoria_sk AND c.activo = TRUE
JOIN dim_film f ON fv.film_sk = f.film_sk AND f.activo = TRUE
GROUP BY c.nombre_categoria
ORDER BY ingresos_totales DESC;

-- Vista: Resumen ejecutivo
DROP VIEW IF EXISTS v_resumen_ejecutivo;
CREATE VIEW v_resumen_ejecutivo AS
SELECT 
    t.anio,
    t.mes,
    COUNT(DISTINCT fv.film_sk) as peliculas_rentadas,
    COUNT(DISTINCT fv.tienda_sk) as tiendas_activas,
    SUM(fv.cantidad_rentas) as total_rentas,
    SUM(fv.monto_total) as ingresos_totales,
    AVG(fv.monto_promedio) as ticket_promedio,
    SUM(fv.cantidad_rentas) / COUNT(DISTINCT fv.tienda_sk) as rentas_por_tienda
FROM fact_ventas fv
JOIN dim_tiempo t ON fv.fecha_id = t.fecha_id
GROUP BY t.anio, t.mes
ORDER BY t.anio, t.mes;