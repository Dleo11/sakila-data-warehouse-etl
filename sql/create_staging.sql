-- ============================================
-- STAGING AREA - Tablas temporales
-- RF2: Staging / área intermedia
-- ============================================

USE sakila_staging;

-- Tabla de control de ejecuciones ETL
CREATE TABLE IF NOT EXISTS etl_control (
    etl_id INT AUTO_INCREMENT PRIMARY KEY,
    proceso VARCHAR(100) NOT NULL,
    fecha_inicio DATETIME NOT NULL,
    fecha_fin DATETIME,
    estado ENUM('INICIADO', 'COMPLETADO', 'ERROR') DEFAULT 'INICIADO',
    registros_leidos INT DEFAULT 0,
    registros_escritos INT DEFAULT 0,
    registros_error INT DEFAULT 0,
    mensaje_error TEXT,
    duracion_segundos INT,
    INDEX idx_proceso (proceso),
    INDEX idx_fecha (fecha_inicio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Rental (rentas)
DROP TABLE IF EXISTS stg_rental;
CREATE TABLE stg_rental (
    rental_id INT,
    rental_date DATETIME,
    inventory_id INT,
    customer_id INT,
    return_date DATETIME,
    staff_id INT,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    es_valido BOOLEAN DEFAULT TRUE,
    mensaje_validacion VARCHAR(255),
    PRIMARY KEY (rental_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Payment (pagos)
DROP TABLE IF EXISTS stg_payment;
CREATE TABLE stg_payment (
    payment_id INT,
    customer_id INT,
    staff_id INT,
    rental_id INT,
    amount DECIMAL(5,2),
    payment_date DATETIME,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    es_valido BOOLEAN DEFAULT TRUE,
    mensaje_validacion VARCHAR(255),
    PRIMARY KEY (payment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Inventory (inventario)
DROP TABLE IF EXISTS stg_inventory;
CREATE TABLE stg_inventory (
    inventory_id INT,
    film_id INT,
    store_id INT,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (inventory_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Film (películas)
DROP TABLE IF EXISTS stg_film;
CREATE TABLE stg_film (
    film_id INT,
    title VARCHAR(255),
    description TEXT,
    release_year INT,
    language_id INT,
    rental_duration INT,
    rental_rate DECIMAL(4,2),
    length INT,
    replacement_cost DECIMAL(5,2),
    rating VARCHAR(10),
    special_features TEXT,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (film_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Film_Category (relación película-categoría)
DROP TABLE IF EXISTS stg_film_category;
CREATE TABLE stg_film_category (
    film_id INT,
    category_id INT,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (film_id, category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Category (categorías)
DROP TABLE IF EXISTS stg_category;
CREATE TABLE stg_category (
    category_id INT,
    name VARCHAR(25),
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Store (tiendas)
DROP TABLE IF EXISTS stg_store;
CREATE TABLE stg_store (
    store_id INT,
    manager_staff_id INT,
    address_id INT,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Address (direcciones)
DROP TABLE IF EXISTS stg_address;
CREATE TABLE stg_address (
    address_id INT,
    address VARCHAR(50),
    address2 VARCHAR(50),
    district VARCHAR(20),
    city_id INT,
    postal_code VARCHAR(10),
    phone VARCHAR(20),
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (address_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: City (ciudades)
DROP TABLE IF EXISTS stg_city;
CREATE TABLE stg_city (
    city_id INT,
    city VARCHAR(50),
    country_id INT,
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (city_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging: Country (países)
DROP TABLE IF EXISTS stg_country;
CREATE TABLE stg_country (
    country_id INT,
    country VARCHAR(50),
    last_update TIMESTAMP,
    -- Metadatos ETL
    etl_fecha_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_id INT,
    PRIMARY KEY (country_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla de auditoría de calidad
CREATE TABLE IF NOT EXISTS audit_calidad (
    audit_id INT AUTO_INCREMENT PRIMARY KEY,
    etl_id INT,
    tabla_origen VARCHAR(100),
    tabla_destino VARCHAR(100),
    validacion VARCHAR(255),
    resultado ENUM('PASS', 'FAIL', 'WARNING'),
    valor_esperado VARCHAR(100),
    valor_obtenido VARCHAR(100),
    mensaje TEXT,
    fecha_validacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_etl (etl_id),
    INDEX idx_resultado (resultado)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Vista de resumen de ejecuciones
CREATE OR REPLACE VIEW v_etl_resumen AS
SELECT 
    etl_id,
    proceso,
    fecha_inicio,
    fecha_fin,
    estado,
    registros_leidos,
    registros_escritos,
    registros_error,
    duracion_segundos,
    ROUND(registros_escritos / NULLIF(registros_leidos, 0) * 100, 2) as tasa_exito_pct
FROM etl_control
ORDER BY etl_id DESC;