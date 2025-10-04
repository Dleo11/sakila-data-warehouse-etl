"""
Módulo de Validaciones de Calidad de Datos
RF8: Validaciones de calidad de datos
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Dict, List, Tuple
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from src.logger_config import ETLLogger

class DataValidator:
    """Validador de calidad de datos en staging"""
    
    def __init__(self, etl_id: int = None):
        """
        Inicializa el validador
        
        Args:
            etl_id: ID de la ejecución ETL actual
        """
        self.etl_logger = ETLLogger('validator', Config.ETL_LOG_PATH, Config.ETL_LOG_LEVEL)
        self.logger = self.etl_logger.get_logger()
        
        self.engine_staging = create_engine(Config.get_staging_connection_string())
        self.etl_id = etl_id
        
        self.logger.info("✅ Validador inicializado correctamente")
    
    def registrar_validacion(self, tabla_origen: str, tabla_destino: str,
                            validacion: str, resultado: str, 
                            valor_esperado: str = None, valor_obtenido: str = None,
                            mensaje: str = None):
        """
        Registra resultado de validación en audit_calidad
        
        Args:
            tabla_origen: Tabla origen
            tabla_destino: Tabla destino
            validacion: Nombre de la validación
            resultado: 'PASS', 'FAIL', 'WARNING'
            valor_esperado: Valor esperado (opcional)
            valor_obtenido: Valor obtenido (opcional)
            mensaje: Mensaje adicional (opcional)
        """
        query = text("""
            INSERT INTO audit_calidad 
            (etl_id, tabla_origen, tabla_destino, validacion, resultado,
             valor_esperado, valor_obtenido, mensaje)
            VALUES (:etl_id, :tabla_origen, :tabla_destino, :validacion, :resultado,
                    :esperado, :obtenido, :mensaje)
        """)
        
        with self.engine_staging.connect() as conn:
            conn.execute(query, {
                "etl_id": self.etl_id,
                "tabla_origen": tabla_origen,
                "tabla_destino": tabla_destino,
                "validacion": validacion,
                "resultado": resultado,
                "esperado": str(valor_esperado) if valor_esperado else None,
                "obtenido": str(valor_obtenido) if valor_obtenido else None,
                "mensaje": mensaje
            })
            conn.commit()
    
    def validar_no_duplicados(self, tabla: str, columnas_pk: List[str]) -> bool:
        """
        Valida que no existan duplicados en las claves primarias
        
        Args:
            tabla: Nombre de la tabla
            columnas_pk: Lista de columnas que forman la PK
            
        Returns:
            True si no hay duplicados, False si los hay
        """
        pk_str = ", ".join(columnas_pk)
        query = text(f"""
            SELECT {pk_str}, COUNT(*) as duplicados
            FROM {tabla}
            GROUP BY {pk_str}
            HAVING COUNT(*) > 1
        """)
        
        with self.engine_staging.connect() as conn:
            df_duplicados = pd.read_sql(query, conn)
        
        if len(df_duplicados) > 0:
            self.logger.warning(f"⚠️  {len(df_duplicados)} claves duplicadas en {tabla}")
            self.registrar_validacion(
                tabla, tabla, "Duplicados PK", "FAIL",
                "0", str(len(df_duplicados)),
                f"Encontrados {len(df_duplicados)} duplicados en {pk_str}"
            )
            return False
        else:
            self.logger.info(f"✅ Sin duplicados en {tabla}")
            self.registrar_validacion(
                tabla, tabla, "Duplicados PK", "PASS",
                "0", "0", "No se encontraron duplicados"
            )
            return True
    
    def validar_valores_nulos(self, tabla: str, columnas_requeridas: List[str]) -> bool:
        """
        Valida que columnas requeridas no tengan valores nulos
        
        Args:
            tabla: Nombre de la tabla
            columnas_requeridas: Lista de columnas que no deben ser NULL
            
        Returns:
            True si todas las columnas están completas, False si hay nulos
        """
        tiene_nulos = False
        
        for columna in columnas_requeridas:
            query = text(f"""
                SELECT COUNT(*) as nulos
                FROM {tabla}
                WHERE {columna} IS NULL
            """)
            
            with self.engine_staging.connect() as conn:
                result = conn.execute(query)
                nulos = result.fetchone()[0]
            
            if nulos > 0:
                self.logger.warning(f"⚠️  {nulos} valores nulos en {tabla}.{columna}")
                self.registrar_validacion(
                    tabla, tabla, f"Nulos en {columna}", "FAIL",
                    "0", str(nulos),
                    f"Columna requerida tiene {nulos} valores nulos"
                )
                tiene_nulos = True
            else:
                self.registrar_validacion(
                    tabla, tabla, f"Nulos en {columna}", "PASS",
                    "0", "0", "Columna sin valores nulos"
                )
        
        if not tiene_nulos:
            self.logger.info(f"✅ Todas las columnas requeridas completas en {tabla}")
        
        return not tiene_nulos
    
    def validar_rangos_numericos(self, tabla: str, columna: str, 
                                 min_val: float = None, max_val: float = None) -> bool:
        """
        Valida que valores numéricos estén en rangos válidos
        
        Args:
            tabla: Nombre de la tabla
            columna: Columna a validar
            min_val: Valor mínimo permitido
            max_val: Valor máximo permitido
            
        Returns:
            True si todos los valores están en rango
        """
        condiciones = []
        if min_val is not None:
            condiciones.append(f"{columna} < {min_val}")
        if max_val is not None:
            condiciones.append(f"{columna} > {max_val}")
        
        if not condiciones:
            return True
        
        where_clause = " OR ".join(condiciones)
        query = text(f"""
            SELECT COUNT(*) as fuera_rango
            FROM {tabla}
            WHERE {where_clause}
        """)
        
        with self.engine_staging.connect() as conn:
            result = conn.execute(query)
            fuera_rango = result.fetchone()[0]
        
        if fuera_rango > 0:
            self.logger.warning(f"⚠️  {fuera_rango} valores fuera de rango en {tabla}.{columna}")
            self.registrar_validacion(
                tabla, tabla, f"Rango {columna}", "FAIL",
                f"[{min_val}, {max_val}]", f"{fuera_rango} fuera de rango",
                f"Valores fuera del rango permitido"
            )
            return False
        else:
            self.logger.info(f"✅ Valores en rango válido: {tabla}.{columna}")
            self.registrar_validacion(
                tabla, tabla, f"Rango {columna}", "PASS",
                f"[{min_val}, {max_val}]", "Todos en rango",
                "Todos los valores dentro del rango"
            )
            return True
    
    def validar_integridad_referencial(self, tabla_hija: str, columna_fk: str,
                                       tabla_padre: str, columna_pk: str) -> bool:
        """
        Valida integridad referencial entre tablas
        
        Args:
            tabla_hija: Tabla con FK
            columna_fk: Columna FK
            tabla_padre: Tabla con PK
            columna_pk: Columna PK
            
        Returns:
            True si no hay llaves huérfanas
        """
        query = text(f"""
            SELECT COUNT(*) as huerfanas
            FROM {tabla_hija} h
            LEFT JOIN {tabla_padre} p ON h.{columna_fk} = p.{columna_pk}
            WHERE p.{columna_pk} IS NULL AND h.{columna_fk} IS NOT NULL
        """)
        
        with self.engine_staging.connect() as conn:
            result = conn.execute(query)
            huerfanas = result.fetchone()[0]
        
        if huerfanas > 0:
            self.logger.error(f"❌ {huerfanas} llaves huérfanas: {tabla_hija}.{columna_fk} → {tabla_padre}.{columna_pk}")
            self.registrar_validacion(
                tabla_hija, tabla_padre, "Integridad Referencial", "FAIL",
                "0", str(huerfanas),
                f"FK {columna_fk} tiene llaves sin correspondencia en {tabla_padre}"
            )
            return False
        else:
            self.logger.info(f"✅ Integridad referencial OK: {tabla_hija}.{columna_fk} → {tabla_padre}.{columna_pk}")
            self.registrar_validacion(
                tabla_hija, tabla_padre, "Integridad Referencial", "PASS",
                "0", "0", "Todas las FK tienen correspondencia"
            )
            return True
    
    def validar_consistencia_totales(self, tabla_origen: str, tabla_destino: str,
                                    columna_suma: str) -> bool:
        """
        Valida que totales sean consistentes entre tablas
        
        Args:
            tabla_origen: Tabla origen
            tabla_destino: Tabla destino
            columna_suma: Columna a sumar
            
        Returns:
            True si los totales coinciden
        """
        query_origen = text(f"SELECT SUM({columna_suma}) as total FROM {tabla_origen}")
        query_destino = text(f"SELECT SUM({columna_suma}) as total FROM {tabla_destino}")
        
        with self.engine_staging.connect() as conn:
            total_origen = conn.execute(query_origen).fetchone()[0] or 0
            total_destino = conn.execute(query_destino).fetchone()[0] or 0
        
        diferencia = abs(total_origen - total_destino)
        porcentaje_dif = (diferencia / total_origen * 100) if total_origen > 0 else 0
        
        if porcentaje_dif > 0.01:  # Tolerancia 0.01%
            self.logger.warning(f"⚠️  Diferencia en totales: {porcentaje_dif:.2f}%")
            self.registrar_validacion(
                tabla_origen, tabla_destino, "Consistencia Totales", "WARNING",
                str(total_origen), str(total_destino),
                f"Diferencia: {diferencia:.2f} ({porcentaje_dif:.2f}%)"
            )
            return False
        else:
            self.logger.info(f"✅ Totales consistentes: {tabla_origen} → {tabla_destino}")
            self.registrar_validacion(
                tabla_origen, tabla_destino, "Consistencia Totales", "PASS",
                str(total_origen), str(total_destino),
                "Totales coinciden"
            )
            return True
    
    def ejecutar_validaciones_staging(self) -> Dict[str, bool]:
        """
        Ejecuta todas las validaciones sobre las tablas de staging
        
        Returns:
            Diccionario con resultados de validaciones
        """
        self.etl_logger.log_etl_start("VALIDACIONES", "Validando calidad de datos en staging")
        
        resultados = {}
        
        # Validar rental
        self.logger.info("🔍 Validando stg_rental...")
        resultados['rental_no_dup'] = self.validar_no_duplicados('stg_rental', ['rental_id'])
        resultados['rental_no_null'] = self.validar_valores_nulos('stg_rental', 
            ['rental_id', 'rental_date', 'inventory_id', 'customer_id'])
        
        # Validar payment
        self.logger.info("🔍 Validando stg_payment...")
        resultados['payment_no_dup'] = self.validar_no_duplicados('stg_payment', ['payment_id'])
        resultados['payment_no_null'] = self.validar_valores_nulos('stg_payment',
            ['payment_id', 'customer_id', 'amount', 'payment_date'])
        resultados['payment_montos'] = self.validar_rangos_numericos('stg_payment', 'amount', 
            min_val=0, max_val=100)
        
        # Validar film
        self.logger.info("🔍 Validando stg_film...")
        resultados['film_no_dup'] = self.validar_no_duplicados('stg_film', ['film_id'])
        resultados['film_rates'] = self.validar_rangos_numericos('stg_film', 'rental_rate',
            min_val=0, max_val=10)
        
        # Validar integridad referencial
        self.logger.info("🔍 Validando integridad referencial...")
        resultados['rental_inventory_fk'] = self.validar_integridad_referencial(
            'stg_rental', 'inventory_id', 'stg_inventory', 'inventory_id')
        resultados['inventory_film_fk'] = self.validar_integridad_referencial(
            'stg_inventory', 'film_id', 'stg_film', 'film_id')
        resultados['store_address_fk'] = self.validar_integridad_referencial(
            'stg_store', 'address_id', 'stg_address', 'address_id')
        
        total_validaciones = len(resultados)
        validaciones_exitosas = sum(resultados.values())
        tasa_exito = (validaciones_exitosas / total_validaciones * 100)
        
        self.etl_logger.log_etl_end("VALIDACIONES", exito=True, detalles={
            'Total validaciones': total_validaciones,
            'Exitosas': validaciones_exitosas,
            'Fallidas': total_validaciones - validaciones_exitosas,
            'Tasa de éxito': f"{tasa_exito:.1f}%"
        })
        
        return resultados
    
    def cerrar_conexion(self):
        """Cierra conexión a staging"""
        self.engine_staging.dispose()
        self.logger.info("🔌 Conexión cerrada")