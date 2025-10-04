"""
Configuración centralizada del proyecto ETL Sakila
Carga variables de entorno desde .env de forma segura
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables de entorno desde .env
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / '.env')

class Config:
    """Clase de configuración centralizada"""
    
    # Configuración Sakila (Origen)
    SAKILA_CONFIG = {
        'host': os.getenv('SAKILA_HOST', 'localhost'),
        'port': int(os.getenv('SAKILA_PORT', 3306)),
        'user': os.getenv('SAKILA_USER'),
        'password': os.getenv('SAKILA_PASSWORD'),
        'database': os.getenv('SAKILA_DATABASE', 'sakila')
    }
    
    # Configuración Data Mart (Destino)
    DM_CONFIG = {
        'host': os.getenv('DM_HOST', 'localhost'),
        'port': int(os.getenv('DM_PORT', 3306)),
        'user': os.getenv('DM_USER'),
        'password': os.getenv('DM_PASSWORD'),
        'database': os.getenv('DM_DATABASE', 'sakila_dw')
    }
    
    # Configuración Staging
    STAGING_CONFIG = {
        'host': os.getenv('DM_HOST', 'localhost'),
        'port': int(os.getenv('DM_PORT', 3306)),
        'user': os.getenv('DM_USER'),
        'password': os.getenv('DM_PASSWORD'),
        'database': os.getenv('STAGING_DATABASE', 'sakila_staging')
    }
    
    # Configuración ETL
    ETL_BATCH_SIZE = int(os.getenv('ETL_BATCH_SIZE', 1000))
    ETL_LOG_LEVEL = os.getenv('ETL_LOG_LEVEL', 'INFO')
    ETL_LOG_PATH = BASE_DIR / os.getenv('ETL_LOG_PATH', 'logs/')
    
    # Rutas del proyecto
    PROJECT_ROOT = BASE_DIR
    SQL_DIR = BASE_DIR / 'sql'
    NOTEBOOKS_DIR = BASE_DIR / 'notebooks'
    
    @staticmethod
    def get_sakila_connection_string():
        """Retorna string de conexión SQLAlchemy para Sakila"""
        cfg = Config.SAKILA_CONFIG
        return f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    
    @staticmethod
    def get_dm_connection_string():
        """Retorna string de conexión SQLAlchemy para Data Mart"""
        cfg = Config.DM_CONFIG
        return f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    
    @staticmethod
    def get_staging_connection_string():
        """Retorna string de conexión SQLAlchemy para Staging"""
        cfg = Config.STAGING_CONFIG
        return f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    
    @staticmethod
    def validate_config():
        """Valida que las configuraciones críticas estén presentes"""
        required_vars = [
            'SAKILA_USER', 'SAKILA_PASSWORD',
            'DM_USER', 'DM_PASSWORD'
        ]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            raise ValueError(f"❌ Variables de entorno faltantes: {', '.join(missing)}\n"
                           f"Por favor configura el archivo .env")
        
        return True

# Validar configuración al importar
try:
    Config.validate_config()
    print("✅ Configuración cargada correctamente")
except ValueError as e:
    print(e)
    raise