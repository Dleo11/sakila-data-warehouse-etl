"""
Módulo de Transformaciones y Carga al Data Mart
RF3-4: Transformaciones analíticas y carga al modelo estrella
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Dict, Tuple
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from src.logger_config import ETLLogger

class DataMartTransformer:
    """Transformador para crear y poblar el modelo estrella"""
    
    def __init__(self, etl_id: int = None):
        """Inicializa el transformador"""
        self.etl_logger = ETLLogger('transformer', Config.ETL_LOG_PATH, Config.ETL_LOG_LEVEL)
        self.logger = self.etl_logger.get_logger()
        
        self.engine_staging = create_engine(Config.get_staging_connection_string())
        self.engine_dm = create_engine(Config.get_dm_connection_string())
        self.etl_id = etl_id
        
        self.logger.info("✅ Transformador inicializado")
    
    def poblar_dim_tiempo(self, fecha_inicio: str = '2005-01-01', 
                         fecha_fin: str = '2026-12-31') -> int:
        """
        Puebla la dimensión tiempo con rango de fechas
        
        Args:
            fecha_inicio: Fecha inicial
            fecha_fin: Fecha final
            
        Returns:
            Número de registros insertados
        """
        self.logger.info(f"🕐 Poblando dim_tiempo ({fecha_inicio} a {fecha_fin})...")
        
        # Generar rango de fechas
        fecha_range = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
        
        # Crear DataFrame
        df_tiempo = pd.DataFrame({
            'fecha': fecha_range,
            'fecha_id': fecha_range.strftime('%Y%m%d').astype(int),
            'anio': fecha_range.year,
            'trimestre': fecha_range.quarter,
            'mes': fecha_range.month,
            'mes_nombre': fecha_range.strftime('%B'),
            'dia': fecha_range.day,
            'dia_semana': fecha_range.dayofweek + 1,
            'dia_semana_nombre': fecha_range.strftime('%A'),
            'semana_anio': fecha_range.isocalendar().week,
            'es_fin_semana': fecha_range.dayofweek >= 5
        })
        
        # Limpiar tabla manualmente si ya existe
        with self.engine_dm.connect() as conn:
            try:
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                conn.execute(text("TRUNCATE TABLE dim_tiempo"))
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                conn.commit()
                self.logger.info("   Tabla dim_tiempo limpiada")
            except:
                self.logger.info("   dim_tiempo no existe aún, se creará")

        # Cargar datos
        registros = df_tiempo.to_sql(
            'dim_tiempo',
            self.engine_dm,
            if_exists='append',  # ← Cambiar a 'append'
            index=False,
            chunksize=1000
        )
        
        self.logger.info(f"✅ dim_tiempo poblado: {len(df_tiempo):,} registros")
        return len(df_tiempo)
    
    def poblar_dim_film(self) -> Tuple[int, int]:
        """
        Puebla dim_film desde staging (SCD Type 2)
        
        Returns:
            (registros_nuevos, registros_actualizados)
        """
        self.logger.info("🎬 Poblando dim_film...")
        
        # Extraer de staging
        query = """
            SELECT DISTINCT
                f.film_id,
                f.title as titulo,
                f.description as descripcion,
                f.release_year as anio_lanzamiento,
                f.length as duracion,
                f.rating as clasificacion,
                f.rental_rate as tarifa_renta,
                f.replacement_cost as costo_reemplazo
            FROM stg_film f
        """
        
        df_film = pd.read_sql(query, self.engine_staging)
        
        # Cargar con SCD Type 2 simplificado
        # Por ahora: INSERT ignorando duplicados
        registros = 0
        actualizados = 0
        
        for _, row in df_film.iterrows():
            # Verificar si existe
            query_check = text("""
                SELECT film_sk, tarifa_renta 
                FROM dim_film 
                WHERE film_id = :film_id AND activo = TRUE
            """)
            
            with self.engine_dm.connect() as conn:
                result = conn.execute(query_check, {"film_id": row['film_id']})
                existente = result.fetchone()
                
                if existente:
                    # SCD Type 2: Si cambió tarifa, cerrar registro y crear nuevo
                    if abs(existente[1] - row['tarifa_renta']) > 0.01:
                        # Cerrar registro anterior
                        query_close = text("""
                            UPDATE dim_film
                            SET fecha_fin = NOW(), activo = FALSE
                            WHERE film_sk = :film_sk
                        """)
                        conn.execute(query_close, {"film_sk": existente[0]})
                        
                        # Insertar nuevo registro
                        query_insert = text("""
                            INSERT INTO dim_film 
                            (film_id, titulo, descripcion, anio_lanzamiento, duracion,
                             clasificacion, tarifa_renta, costo_reemplazo, version)
                            VALUES (:film_id, :titulo, :desc, :anio, :duracion,
                                    :clasif, :tarifa, :costo, :version)
                        """)
                        conn.execute(query_insert, {
                            "film_id": row['film_id'],
                            "titulo": row['titulo'],
                            "desc": row['descripcion'],
                            "anio": row['anio_lanzamiento'],
                            "duracion": row['duracion'],
                            "clasif": row['clasificacion'],
                            "tarifa": row['tarifa_renta'],
                            "costo": row['costo_reemplazo'],
                            "version": 2
                        })
                        actualizados += 1
                else:
                    # Insertar nuevo film
                    query_insert = text("""
                        INSERT INTO dim_film 
                        (film_id, titulo, descripcion, anio_lanzamiento, duracion,
                         clasificacion, tarifa_renta, costo_reemplazo)
                        VALUES (:film_id, :titulo, :desc, :anio, :duracion,
                                :clasif, :tarifa, :costo)
                    """)
                    conn.execute(query_insert, {
                        "film_id": row['film_id'],
                        "titulo": row['titulo'],
                        "desc": row['descripcion'],
                        "anio": row['anio_lanzamiento'],
                        "duracion": row['duracion'],
                        "clasif": row['clasificacion'],
                        "tarifa": row['tarifa_renta'],
                        "costo": row['costo_reemplazo']
                    })
                    registros += 1
                
                conn.commit()
        
        self.logger.info(f"✅ dim_film: {registros} nuevos, {actualizados} actualizados")
        return registros, actualizados
    
    def poblar_dim_categoria(self) -> int:
        """Puebla dim_categoria desde staging"""
        self.logger.info("📂 Poblando dim_categoria...")
        
        query = """
            SELECT DISTINCT
                category_id as categoria_id,
                name as nombre_categoria
            FROM stg_category
        """
        
        df_categoria = pd.read_sql(query, self.engine_staging)
        
        with self.engine_dm.connect() as conn:
            try:
                conn.execute(text("TRUNCATE TABLE dim_categoria"))
                conn.commit()
            except:
                pass

        registros = df_categoria.to_sql(
            'dim_categoria',
            self.engine_dm,
            if_exists='append',
            index=False
        )
        
        self.logger.info(f"✅ dim_categoria: {len(df_categoria)} registros")
        return len(df_categoria)
    
    def poblar_dim_tienda(self) -> int:
        """Puebla dim_tienda desde staging"""
        self.logger.info("🏪 Poblando dim_tienda...")
        
        query = """
            SELECT DISTINCT
                s.store_id as tienda_id,
                CONCAT('Tienda ', s.store_id) as nombre_tienda,
                a.address as direccion,
                c.city as ciudad,
                co.country as pais,
                a.postal_code as codigo_postal
            FROM stg_store s
            LEFT JOIN stg_address a ON s.address_id = a.address_id
            LEFT JOIN stg_city c ON a.city_id = c.city_id
            LEFT JOIN stg_country co ON c.country_id = co.country_id
        """
        
        df_tienda = pd.read_sql(query, self.engine_staging)
        
        with self.engine_dm.connect() as conn:
            try:
                conn.execute(text("TRUNCATE TABLE dim_tienda"))
                conn.commit()
            except:
                pass

        registros = df_tienda.to_sql(
            'dim_tienda',
            self.engine_dm,
            if_exists='append',
            index=False
        )
        
        self.logger.info(f"✅ dim_tienda: {len(df_tienda)} registros")
        return len(df_tienda)
    
    def poblar_fact_ventas(self) -> int:
        """
        Puebla fact_ventas con datos agregados
        
        Returns:
            Número de registros insertados
        """
        self.logger.info("💰 Poblando fact_ventas...")
        
        query = text("""
            INSERT INTO fact_ventas 
            (fecha_id, film_sk, categoria_sk, tienda_sk, cantidad_rentas, 
             monto_total, monto_promedio, dias_renta_promedio, cantidad_devoluciones, etl_id)
            SELECT 
                DATE_FORMAT(r.rental_date, '%Y%m%d') as fecha_id,
                df.film_sk,
                dc.categoria_sk,
                dt.tienda_sk,
                COUNT(DISTINCT r.rental_id) as cantidad_rentas,
                SUM(COALESCE(p.amount, 0)) as monto_total,
                AVG(COALESCE(p.amount, 0)) as monto_promedio,
                AVG(COALESCE(DATEDIFF(r.return_date, r.rental_date), 0)) as dias_renta_promedio,
                SUM(CASE WHEN r.return_date IS NOT NULL THEN 1 ELSE 0 END) as cantidad_devoluciones,
                :etl_id as etl_id
            FROM sakila_staging.stg_rental r
            INNER JOIN sakila_staging.stg_inventory i ON r.inventory_id = i.inventory_id
            INNER JOIN sakila_staging.stg_film f ON i.film_id = f.film_id
            INNER JOIN sakila_staging.stg_film_category fc ON f.film_id = fc.film_id
            LEFT JOIN sakila_staging.stg_payment p ON r.rental_id = p.rental_id
            -- Joins a dimensiones (solo activos)
            INNER JOIN sakila_dw.dim_film df ON f.film_id = df.film_id AND df.activo = TRUE
            INNER JOIN sakila_dw.dim_categoria dc ON fc.category_id = dc.categoria_id AND dc.activo = TRUE
            INNER JOIN sakila_dw.dim_tienda dt ON i.store_id = dt.tienda_id AND dt.activo = TRUE
            WHERE r.es_valido = TRUE OR r.es_valido IS NULL
            GROUP BY 
                DATE_FORMAT(r.rental_date, '%Y%m%d'),
                df.film_sk,
                dc.categoria_sk,
                dt.tienda_sk
        """)
        
        with self.engine_dm.connect() as conn:
            result = conn.execute(query, {"etl_id": self.etl_id})
            conn.commit()
            registros = result.rowcount
        
        self.logger.info(f"✅ fact_ventas: {registros:,} registros")
        return registros
    
    def ejecutar_transformacion_completa(self) -> Dict[str, int]:
        """
        Ejecuta todo el proceso de transformación
        
        Returns:
            Estadísticas de transformación
        """
        self.etl_logger.log_etl_start("TRANSFORMACION_DM", 
                                     "Creando modelo estrella")
        
        estadisticas = {}
        
        try:
            # 1. Poblar dimensiones
            estadisticas['dim_tiempo'] = self.poblar_dim_tiempo()
            
            nuevos, actualizados = self.poblar_dim_film()
            estadisticas['dim_film_nuevos'] = nuevos
            estadisticas['dim_film_actualizados'] = actualizados
            
            estadisticas['dim_categoria'] = self.poblar_dim_categoria()
            estadisticas['dim_tienda'] = self.poblar_dim_tienda()
            
            # 2. Poblar hechos
            estadisticas['fact_ventas'] = self.poblar_fact_ventas()
            
            self.etl_logger.log_etl_end("TRANSFORMACION_DM", exito=True, detalles={
                'Dimensiones pobladas': 4,
                'Registros en fact_ventas': f"{estadisticas['fact_ventas']:,}",
                'Films nuevos': estadisticas['dim_film_nuevos'],
                'Films actualizados': estadisticas['dim_film_actualizados']
            })
            
            return estadisticas
            
        except Exception as e:
            self.etl_logger.log_etl_end("TRANSFORMACION_DM", exito=False, detalles={
                'Error': str(e)
            })
            raise
    
    def cerrar_conexiones(self):
        """Cierra conexiones"""
        self.engine_staging.dispose()
        self.engine_dm.dispose()
        self.logger.info("🔌 Conexiones cerradas")