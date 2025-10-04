"""
Módulo de Procesamiento de Staging
RF2: Staging / área intermedia - transformaciones y limpieza
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Dict, List
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from src.logger_config import ETLLogger

class StagingProcessor:
    """Procesador de datos en staging - limpieza y transformaciones"""
    
    def __init__(self, etl_id: int = None):
        """
        Inicializa el procesador de staging
        
        Args:
            etl_id: ID de la ejecución ETL actual
        """
        self.etl_logger = ETLLogger('staging', Config.ETL_LOG_PATH, Config.ETL_LOG_LEVEL)
        self.logger = self.etl_logger.get_logger()
        
        self.engine_staging = create_engine(Config.get_staging_connection_string())
        self.etl_id = etl_id
        
        self.logger.info("✅ Procesador de staging inicializado")
    
    def limpiar_datos_nulos(self, tabla: str, columnas_numericas: List[str] = None,
                           columnas_texto: List[str] = None) -> int:
        """
        Limpia valores nulos reemplazándolos por defaults
        
        Args:
            tabla: Nombre de la tabla
            columnas_numericas: Columnas numéricas (rellenar con 0)
            columnas_texto: Columnas de texto (rellenar con '')
            
        Returns:
            Número de registros actualizados
        """
        total_actualizados = 0
        
        # Limpiar columnas numéricas
        if columnas_numericas:
            for col in columnas_numericas:
                query = text(f"""
                    UPDATE {tabla}
                    SET {col} = 0
                    WHERE {col} IS NULL
                """)
                
                with self.engine_staging.connect() as conn:
                    result = conn.execute(query)
                    conn.commit()
                    actualizados = result.rowcount
                    
                    if actualizados > 0:
                        self.logger.info(f"   Limpiados {actualizados} nulos en {tabla}.{col}")
                        total_actualizados += actualizados
        
        # Limpiar columnas de texto
        if columnas_texto:
            for col in columnas_texto:
                query = text(f"""
                    UPDATE {tabla}
                    SET {col} = ''
                    WHERE {col} IS NULL
                """)
                
                with self.engine_staging.connect() as conn:
                    result = conn.execute(query)
                    conn.commit()
                    actualizados = result.rowcount
                    
                    if actualizados > 0:
                        self.logger.info(f"   Limpiados {actualizados} nulos en {tabla}.{col}")
                        total_actualizados += actualizados
        
        if total_actualizados > 0:
            self.logger.info(f"✅ Total limpiados en {tabla}: {total_actualizados}")
        
        return total_actualizados
    
    def eliminar_duplicados(self, tabla: str, columnas_pk: List[str]) -> int:
        """
        Elimina registros duplicados basándose en PK
        
        Args:
            tabla: Nombre de la tabla
            columnas_pk: Columnas que forman la PK
            
        Returns:
            Número de duplicados eliminados
        """
        pk_str = ", ".join(columnas_pk)
        
        # Crear columna temporal con row_number
        query = text(f"""
            DELETE t1 FROM {tabla} t1
            INNER JOIN {tabla} t2 
            WHERE t1.etl_fecha_carga > t2.etl_fecha_carga
            AND {' AND '.join([f't1.{col} = t2.{col}' for col in columnas_pk])}
        """)
        
        with self.engine_staging.connect() as conn:
            result = conn.execute(query)
            conn.commit()
            eliminados = result.rowcount
        
        if eliminados > 0:
            self.logger.info(f"✅ Eliminados {eliminados} duplicados de {tabla}")
        else:
            self.logger.info(f"✅ Sin duplicados en {tabla}")
        
        return eliminados
    
    def marcar_registros_invalidos(self, tabla: str, condicion: str, 
                                   mensaje: str) -> int:
        """
        Marca registros como inválidos según condición
        
        Args:
            tabla: Nombre de la tabla
            condicion: Condición SQL para identificar registros inválidos
            mensaje: Mensaje de validación
            
        Returns:
            Número de registros marcados
        """
        query = text(f"""
            UPDATE {tabla}
            SET es_valido = FALSE,
                mensaje_validacion = :mensaje
            WHERE {condicion}
        """)
        
        with self.engine_staging.connect() as conn:
            result = conn.execute(query, {"mensaje": mensaje})
            conn.commit()
            marcados = result.rowcount
        
        if marcados > 0:
            self.logger.warning(f"⚠️  Marcados {marcados} registros inválidos en {tabla}: {mensaje}")
        
        return marcados
    
    def normalizar_textos(self, tabla: str, columnas: List[str]) -> int:
        """
        Normaliza textos: trim, uppercase/lowercase, etc.
        
        Args:
            tabla: Nombre de la tabla
            columnas: Columnas a normalizar
            
        Returns:
            Número de registros actualizados
        """
        total_actualizados = 0
        
        for col in columnas:
            query = text(f"""
                UPDATE {tabla}
                SET {col} = TRIM({col})
                WHERE {col} != TRIM({col})
            """)
            
            with self.engine_staging.connect() as conn:
                result = conn.execute(query)
                conn.commit()
                actualizados = result.rowcount
                
                if actualizados > 0:
                    self.logger.info(f"   Normalizados {actualizados} registros en {tabla}.{col}")
                    total_actualizados += actualizados
        
        if total_actualizados > 0:
            self.logger.info(f"✅ Total normalizados en {tabla}: {total_actualizados}")
        
        return total_actualizados
    
    def convertir_tipos_datos(self, tabla: str) -> pd.DataFrame:
        """
        Lee tabla y asegura tipos de datos correctos
        
        Args:
            tabla: Nombre de la tabla
            
        Returns:
            DataFrame con tipos corregidos
        """
        query = f"SELECT * FROM {tabla}"
        df = pd.read_sql(query, self.engine_staging)
        
        # Convertir fechas
        columnas_fecha = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        for col in columnas_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        self.logger.info(f"✅ Tipos de datos verificados en {tabla}")
        return df
    
    def procesar_rental(self) -> Dict[str, int]:
        """
        Procesa tabla stg_rental con limpieza y validaciones
        
        Returns:
            Estadísticas del procesamiento
        """
        self.logger.info("🔧 Procesando stg_rental...")
        stats = {}
        
        # Eliminar duplicados
        stats['duplicados'] = self.eliminar_duplicados('stg_rental', ['rental_id'])
        
        # Marcar registros sin return_date como válidos (rentas activas)
        # pero sin fecha de devolución específica
        
        # Marcar inválidos: rental_date futura
        stats['fecha_invalida'] = self.marcar_registros_invalidos(
            'stg_rental',
            'rental_date > NOW()',
            'Fecha de renta futura'
        )
        
        # Marcar inválidos: return_date < rental_date
        stats['return_invalido'] = self.marcar_registros_invalidos(
            'stg_rental',
            'return_date IS NOT NULL AND return_date < rental_date',
            'Fecha de devolución antes de renta'
        )
        
        self.logger.info(f"✅ stg_rental procesado: {stats}")
        return stats
    
    def procesar_payment(self) -> Dict[str, int]:
        """
        Procesa tabla stg_payment con limpieza y validaciones
        
        Returns:
            Estadísticas del procesamiento
        """
        self.logger.info("🔧 Procesando stg_payment...")
        stats = {}
        
        # Eliminar duplicados
        stats['duplicados'] = self.eliminar_duplicados('stg_payment', ['payment_id'])
        
        # Marcar inválidos: montos negativos
        stats['monto_negativo'] = self.marcar_registros_invalidos(
            'stg_payment',
            'amount < 0',
            'Monto negativo'
        )
        
        # Marcar inválidos: montos excesivos (> $100)
        stats['monto_excesivo'] = self.marcar_registros_invalidos(
            'stg_payment',
            'amount > 100',
            'Monto excesivo (>$100)'
        )
        
        # Marcar inválidos: fecha de pago futura
        stats['fecha_invalida'] = self.marcar_registros_invalidos(
            'stg_payment',
            'payment_date > NOW()',
            'Fecha de pago futura'
        )
        
        self.logger.info(f"✅ stg_payment procesado: {stats}")
        return stats
    
    def procesar_film(self) -> Dict[str, int]:
        """
        Procesa tabla stg_film con limpieza y validaciones
        
        Returns:
            Estadísticas del procesamiento
        """
        self.logger.info("🔧 Procesando stg_film...")
        stats = {}
        
        # Eliminar duplicados
        stats['duplicados'] = self.eliminar_duplicados('stg_film', ['film_id'])
        
        # Normalizar títulos
        stats['normalizados'] = self.normalizar_textos('stg_film', ['title'])
        
        # Marcar inválidos: rental_rate negativo
        stats['rate_negativo'] = self.marcar_registros_invalidos(
            'stg_film',
            'rental_rate < 0',
            'Tarifa de renta negativa'
        )
        
        # Marcar inválidos: duración inválida
        stats['duracion_invalida'] = self.marcar_registros_invalidos(
            'stg_film',
            'length <= 0 OR length > 500',
            'Duración inválida'
        )
        
        self.logger.info(f"✅ stg_film procesado: {stats}")
        return stats
    
    def procesar_todas_las_tablas(self) -> Dict[str, Dict[str, int]]:
        """
        Procesa todas las tablas de staging
        
        Returns:
            Diccionario con estadísticas por tabla
        """
        self.etl_logger.log_etl_start("PROCESAMIENTO_STAGING", 
                                     "Limpieza y transformaciones en staging")
        
        resultados = {}
        
        # Procesar tablas principales
        resultados['rental'] = self.procesar_rental()
        resultados['payment'] = self.procesar_payment()
        resultados['film'] = self.procesar_film()
        
        # Procesar tablas de dimensiones (sin validaciones complejas)
        self.logger.info("🔧 Procesando dimensiones...")
        resultados['category'] = {
            'duplicados': self.eliminar_duplicados('stg_category', ['category_id']),
            'normalizados': self.normalizar_textos('stg_category', ['name'])
        }
        
        resultados['store'] = {
            'duplicados': self.eliminar_duplicados('stg_store', ['store_id'])
        }
        
        resultados['city'] = {
            'duplicados': self.eliminar_duplicados('stg_city', ['city_id']),
            'normalizados': self.normalizar_textos('stg_city', ['city'])
        }
        
        resultados['country'] = {
            'duplicados': self.eliminar_duplicados('stg_country', ['country_id']),
            'normalizados': self.normalizar_textos('stg_country', ['country'])
        }
        
        # Resumen
        total_duplicados = sum(r.get('duplicados', 0) for r in resultados.values())
        total_invalidos = sum(
            sum(v for k, v in r.items() if 'invalido' in k or 'negativo' in k or 'excesivo' in k)
            for r in resultados.values()
        )
        
        self.etl_logger.log_etl_end("PROCESAMIENTO_STAGING", exito=True, detalles={
            'Tablas procesadas': len(resultados),
            'Duplicados eliminados': total_duplicados,
            'Registros inválidos': total_invalidos
        })
        
        return resultados
    
    def obtener_registros_validos(self, tabla: str) -> pd.DataFrame:
        """
        Obtiene solo registros válidos de una tabla
        
        Args:
            tabla: Nombre de la tabla
            
        Returns:
            DataFrame con registros válidos
        """
        # Verificar si la tabla tiene columna es_valido
        query_check = text(f"""
            SELECT COUNT(*) as tiene_columna
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'sakila_staging'
            AND TABLE_NAME = '{tabla}'
            AND COLUMN_NAME = 'es_valido'
        """)
        
        with self.engine_staging.connect() as conn:
            tiene_validacion = conn.execute(query_check).fetchone()[0] > 0
        
        if tiene_validacion:
            query = f"SELECT * FROM {tabla} WHERE es_valido = TRUE"
        else:
            query = f"SELECT * FROM {tabla}"
        
        df = pd.read_sql(query, self.engine_staging)
        self.logger.info(f"📊 {len(df):,} registros válidos en {tabla}")
        
        return df
    
    def cerrar_conexion(self):
        """Cierra conexión a staging"""
        self.engine_staging.dispose()
        self.logger.info("🔌 Conexión cerrada")