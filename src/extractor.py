"""
Módulo de Extracción de Datos desde Sakila
RF1: Extracción de datos de Sakila
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Dict, List, Optional
import sys
from pathlib import Path

# Agregar path del proyecto
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from src.logger_config import get_logger

class SakilaExtractor:
    """Extractor de datos desde la base de datos Sakila"""
    
    def __init__(self):
        """Inicializa el extractor con conexiones y logger"""
        from src.logger_config import ETLLogger
        self.etl_logger = ETLLogger('extractor', Config.ETL_LOG_PATH, Config.ETL_LOG_LEVEL)
        self.logger = self.etl_logger.get_logger()
        
        # Conexiones
        self.engine_sakila = create_engine(Config.get_sakila_connection_string())
        self.engine_staging = create_engine(Config.get_staging_connection_string())
        
        # ID de ejecución actual
        self.etl_id = None
        
        self.logger.info("✅ Extractor inicializado correctamente")
    
    def registrar_inicio_etl(self, proceso: str) -> int:
        """
        Registra el inicio de un proceso ETL en la tabla de control
        
        Args:
            proceso: Nombre del proceso
            
        Returns:
            etl_id generado
        """
        query = text("""
            INSERT INTO etl_control (proceso, fecha_inicio, estado)
            VALUES (:proceso, :fecha_inicio, 'INICIADO')
        """)
        
        with self.engine_staging.connect() as conn:
            result = conn.execute(
                query, 
                {"proceso": proceso, "fecha_inicio": datetime.now()}
            )
            conn.commit()
            etl_id = result.lastrowid
        
        self.etl_id = etl_id
        self.logger.info(f"📝 ETL registrado con ID: {etl_id}")
        return etl_id
    
    def registrar_fin_etl(self, estado: str, registros_leidos: int = 0, 
                         registros_escritos: int = 0, registros_error: int = 0,
                         mensaje_error: str = None):
        """
        Registra el fin de un proceso ETL
        
        Args:
            estado: 'COMPLETADO' o 'ERROR'
            registros_leidos: Total de registros leídos
            registros_escritos: Total de registros escritos
            registros_error: Total de errores
            mensaje_error: Descripción del error (opcional)
        """
        query = text("""
            UPDATE etl_control 
            SET fecha_fin = :fecha_fin,
                estado = :estado,
                registros_leidos = :leidos,
                registros_escritos = :escritos,
                registros_error = :errores,
                mensaje_error = :mensaje,
                duracion_segundos = TIMESTAMPDIFF(SECOND, fecha_inicio, :fecha_fin)
            WHERE etl_id = :etl_id
        """)
        
        with self.engine_staging.connect() as conn:
            conn.execute(query, {
                "etl_id": self.etl_id,
                "fecha_fin": datetime.now(),
                "estado": estado,
                "leidos": registros_leidos,
                "escritos": registros_escritos,
                "errores": registros_error,
                "mensaje": mensaje_error
            })
            conn.commit()
        
        self.logger.info(f"📝 ETL {self.etl_id} finalizado con estado: {estado}")
    
    def extraer_tabla(self, tabla: str, query: str = None, 
                     fecha_desde: datetime = None) -> pd.DataFrame:
        """
        Extrae datos de una tabla de Sakila
        
        Args:
            tabla: Nombre de la tabla
            query: Query personalizado (opcional)
            fecha_desde: Fecha para extracción incremental (opcional)
            
        Returns:
            DataFrame con los datos extraídos
        """
        try:
            if query is None:
                # Caso especial: tabla address (excluir campo GEOMETRY)
                if tabla == 'address':
                    campos = "address_id, address, address2, district, city_id, postal_code, phone, last_update"
                    if fecha_desde:
                        query = f"""
                            SELECT {campos} FROM {tabla} 
                            WHERE last_update >= '{fecha_desde.strftime('%Y-%m-%d %H:%M:%S')}'
                        """
                        self.logger.info(f"📥 Extrayendo {tabla} (incremental desde {fecha_desde})")
                    else:
                        query = f"SELECT {campos} FROM {tabla}"
                        self.logger.info(f"📥 Extrayendo {tabla} (completo, sin campo GEOMETRY)")
                else:
                    # Query por defecto: extracción total
                    if fecha_desde:
                        query = f"""
                            SELECT * FROM {tabla} 
                            WHERE last_update >= '{fecha_desde.strftime('%Y-%m-%d %H:%M:%S')}'
                        """
                        self.logger.info(f"📥 Extrayendo {tabla} (incremental desde {fecha_desde})")
                    else:
                        query = f"SELECT * FROM {tabla}"
                        self.logger.info(f"📥 Extrayendo {tabla} (completo)")
            
            # Ejecutar query
            df = pd.read_sql(query, self.engine_sakila)
            
            self.logger.info(f"✅ Extraídos {len(df):,} registros de {tabla}")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error extrayendo {tabla}: {e}")
            raise
    
    def cargar_a_staging(self, df: pd.DataFrame, tabla_staging: str, 
                        if_exists: str = 'replace') -> int:
        """
        Carga datos al área de staging
        
        Args:
            df: DataFrame a cargar
            tabla_staging: Nombre de la tabla en staging
            if_exists: 'replace' o 'append'
            
        Returns:
            Número de registros cargados
        """
        try:
            # Agregar metadatos ETL
            df_staging = df.copy()
            df_staging['etl_fecha_carga'] = datetime.now()
            df_staging['etl_id'] = self.etl_id
            
            # Cargar a staging
            registros = df_staging.to_sql(
                tabla_staging,
                self.engine_staging,
                if_exists=if_exists,
                index=False,
                chunksize=Config.ETL_BATCH_SIZE
            )
            
            self.logger.info(f"✅ Cargados {len(df_staging):,} registros a {tabla_staging}")
            return len(df_staging)
            
        except Exception as e:
            self.logger.error(f"❌ Error cargando a {tabla_staging}: {e}")
            raise
    
    def extraer_todas_las_tablas(self, incremental: bool = False, 
                                 fecha_desde: datetime = None) -> Dict[str, int]:
        """
        Extrae todas las tablas necesarias de Sakila a Staging
        
        Args:
            incremental: Si True, extrae solo registros nuevos
            fecha_desde: Fecha de inicio para extracción incremental
            
        Returns:
            Diccionario con estadísticas de extracción
        """
        # Tablas a extraer
        tablas = [
            ('rental', 'stg_rental'),
            ('payment', 'stg_payment'),
            ('inventory', 'stg_inventory'),
            ('film', 'stg_film'),
            ('film_category', 'stg_film_category'),
            ('category', 'stg_category'),
            ('store', 'stg_store'),
            ('address', 'stg_address'),
            ('city', 'stg_city'),
            ('country', 'stg_country')
        ]
        
        proceso = f"EXTRACCION_{'INCREMENTAL' if incremental else 'COMPLETA'}"
        self.registrar_inicio_etl(proceso)
        
        estadisticas = {}
        total_leidos = 0
        total_escritos = 0
        errores = 0
        
        self.etl_logger.log_etl_start(proceso, f"Extrayendo {len(tablas)} tablas")
        
        try:
            for tabla_origen, tabla_staging in tablas:
                try:
                    # Extraer de Sakila
                    df = self.extraer_tabla(
                        tabla_origen, 
                        fecha_desde=fecha_desde if incremental else None
                    )
                    
                    registros_leidos = len(df)
                    
                    # Cargar a Staging
                    if registros_leidos > 0:
                        registros_escritos = self.cargar_a_staging(
                            df, 
                            tabla_staging,
                            if_exists='append' if incremental else 'replace'
                        )
                    else:
                        registros_escritos = 0
                        self.logger.info(f"⚠️  No hay datos nuevos en {tabla_origen}")
                    
                    # Estadísticas
                    estadisticas[tabla_origen] = {
                        'leidos': registros_leidos,
                        'escritos': registros_escritos
                    }
                    
                    total_leidos += registros_leidos
                    total_escritos += registros_escritos
                    
                    self.etl_logger.log_table_stats (
                        tabla_origen, 
                        registros_leidos, 
                        registros_escritos
                    )
                    
                except Exception as e:
                    self.logger.error(f"❌ Error en tabla {tabla_origen}: {e}")
                    errores += 1
                    estadisticas[tabla_origen] = {
                        'leidos': 0,
                        'escritos': 0,
                        'error': str(e)
                    }
            
            # Registrar fin exitoso
            self.registrar_fin_etl(
                estado='COMPLETADO',
                registros_leidos=total_leidos,
                registros_escritos=total_escritos,
                registros_error=errores
            )
            
            self.etl_logger.log_etl_end(proceso, exito=True, detalles={
                'Total leídos': f"{total_leidos:,}",
                'Total escritos': f"{total_escritos:,}",
                'Errores': errores
            })
            
            return estadisticas
            
        except Exception as e:
            self.registrar_fin_etl(
                estado='ERROR',
                registros_leidos=total_leidos,
                registros_escritos=total_escritos,
                registros_error=errores,
                mensaje_error=str(e)
            )
            
            self.logger.log_etl_end(proceso, exito=False, detalles={
                'Error': str(e)
            })
            raise
    
    def obtener_ultima_extraccion(self) -> Optional[datetime]:
        """
        Obtiene la fecha de la última extracción exitosa
        
        Returns:
            datetime de la última extracción o None
        """
        query = text("""
            SELECT MAX(fecha_fin) as ultima_fecha
            FROM etl_control
            WHERE estado = 'COMPLETADO' 
            AND proceso LIKE 'EXTRACCION%'
        """)
        
        with self.engine_staging.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            if row and row[0]:
                self.logger.info(f"📅 Última extracción: {row[0]}")
                return row[0]
            else:
                self.logger.info("📅 No hay extracciones previas")
                return None
    
    def cerrar_conexiones(self):
        """Cierra las conexiones a las bases de datos"""
        self.engine_sakila.dispose()
        self.engine_staging.dispose()
        self.logger.info("🔌 Conexiones cerradas")

# Función helper para uso rápido
def extraer_datos(incremental: bool = False) -> Dict[str, int]:
    """
    Función de conveniencia para extraer datos de Sakila
    
    Args:
        incremental: Si True, extrae solo datos nuevos
        
    Returns:
        Diccionario con estadísticas
    """
    extractor = SakilaExtractor()
    
    try:
        if incremental:
            fecha_desde = extractor.obtener_ultima_extraccion()
            if fecha_desde is None:
                extractor.logger.warning("⚠️  No hay fecha previa, haciendo extracción completa")
                incremental = False
        else:
            fecha_desde = None
        
        estadisticas = extractor.extraer_todas_las_tablas(incremental, fecha_desde)
        return estadisticas
        
    finally:
        extractor.cerrar_conexiones()