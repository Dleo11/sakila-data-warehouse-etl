"""
Módulo de conexión a base de datos para Streamlit
Reutiliza la configuración existente del proyecto ETL
"""
import sys
from pathlib import Path
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

# Agregar el directorio raíz al path para importar config
root_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_path))

from config.config import Config

class DatabaseConnection:
    """Clase para manejar conexiones a la base de datos en Streamlit"""
    
    def __init__(self):
        """Inicializa la configuración"""
        self.config = Config()
    
    @st.cache_resource
    def get_engine(_self, database='sakila_dw'):
        """
        Obtiene el engine de SQLAlchemy con cache
        
        Args:
            database: Nombre de la base de datos (sakila_dw por defecto)
        
        Returns:
            Engine de SQLAlchemy
        """
        try:
            # Mapear el nombre de la base de datos al método correcto
            if database == 'sakila_dw' or database == 'datamart':
                connection_string = Config.get_dm_connection_string()
            elif database == 'sakila_staging' or database == 'staging':
                connection_string = Config.get_staging_connection_string()
            elif database == 'sakila' or database == 'source':
                connection_string = Config.get_sakila_connection_string()
            else:
                raise ValueError(f"Base de datos no reconocida: {database}")
            
            engine = create_engine(
                connection_string,
                poolclass=NullPool,  # No usar pool en Streamlit
                echo=False
            )
            
            # Probar conexión
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            return engine
            
        except Exception as e:
            st.error(f"❌ Error al conectar con {database}: {str(e)}")
            st.stop()
    
    @st.cache_data(ttl=300)  # Cache de 5 minutos
    def execute_query(_self, query: str, database: str = 'sakila_dw') -> pd.DataFrame:
        """
        Ejecuta una query y retorna un DataFrame
        
        Args:
            query: Query SQL a ejecutar
            database: Base de datos objetivo
        
        Returns:
            DataFrame con los resultados
        """
        try:
            engine = _self.get_engine(database)
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            st.error(f"❌ Error al ejecutar query: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=600)  # Cache de 10 minutos
    def get_table(_self, table_name: str, database: str = 'sakila_dw') -> pd.DataFrame:
        """
        Obtiene todos los datos de una tabla
        
        Args:
            table_name: Nombre de la tabla
            database: Base de datos objetivo
        
        Returns:
            DataFrame con todos los registros
        """
        query = f"SELECT * FROM {table_name}"
        return _self.execute_query(query, database)
    
    def test_connection(self) -> dict:
        """
        Prueba la conexión a todas las bases de datos
        
        Returns:
            Diccionario con el estado de cada conexión
        """
        databases = {
            'sakila_dw': 'sakila_dw',
            'sakila_staging': 'sakila_staging', 
            'sakila': 'sakila'
        }
        results = {}
        
        for display_name, db_name in databases.items():
            try:
                engine = self.get_engine(db_name)
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT DATABASE() as db_name"))
                    db_connected = result.fetchone()[0]
                results[display_name] = f"✅ Conectado a: {db_connected}"
            except Exception as e:
                results[display_name] = f"❌ Error: {str(e)}"
        
        return results

# Instancia global
db = DatabaseConnection()