"""
Sistema de logging centralizado para ETL
RF7: Auditoría / logging
"""

import logging
import colorlog
from datetime import datetime
from pathlib import Path
import sys

class ETLLogger:
    """Manejador centralizado de logs para el proceso ETL"""
    
    def __init__(self, name: str, log_dir: Path, level: str = 'INFO'):
        """
        Inicializa el logger con configuración dual (archivo + consola)
        
        Args:
            name: Nombre del logger (ej: 'extractor', 'transformer')
            log_dir: Directorio donde guardar logs
            level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Crear logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Evitar duplicación de handlers
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Handler para archivo con timestamp
        log_file = self.log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Handler para consola con colores
        console_handler = colorlog.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formato para archivo (detallado)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Formato para consola (con colores)
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s | %(name)s | %(levelname)s%(reset)s | %(message)s',
            datefmt='%H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
        
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        """Retorna el logger configurado"""
        return self.logger
    
    def log_etl_start(self, proceso: str, descripcion: str = ""):
        """Log de inicio de proceso ETL"""
        self.logger.info("=" * 80)
        self.logger.info(f"🚀 INICIO: {proceso}")
        if descripcion:
            self.logger.info(f"   {descripcion}")
        self.logger.info("=" * 80)
    
    def log_etl_end(self, proceso: str, exito: bool = True, detalles: dict = None):
        """Log de fin de proceso ETL"""
        status = "✅ ÉXITO" if exito else "❌ ERROR"
        self.logger.info("-" * 80)
        self.logger.info(f"{status}: {proceso}")
        
        if detalles:
            for key, value in detalles.items():
                self.logger.info(f"   {key}: {value}")
        
        self.logger.info("=" * 80 + "\n")
    
    def log_table_stats(self, tabla: str, filas_leidas: int, filas_escritas: int = None, errores: int = 0):
        """Log de estadísticas de tabla procesada"""
        self.logger.info(f"📊 Tabla: {tabla}")
        self.logger.info(f"   Filas leídas: {filas_leidas:,}")
        if filas_escritas is not None:
            self.logger.info(f"   Filas escritas: {filas_escritas:,}")
        if errores > 0:
            self.logger.warning(f"   ⚠️  Errores: {errores}")
    
    def log_validation(self, validacion: str, pasado: bool, detalles: str = ""):
        """Log de validación de calidad"""
        status = "✅ PASÓ" if pasado else "❌ FALLÓ"
        level = logging.INFO if pasado else logging.ERROR
        self.logger.log(level, f"{status}: {validacion}")
        if detalles:
            self.logger.log(level, f"   {detalles}")

# Función helper para crear loggers rápidamente
def get_logger(name: str, log_dir: Path = None, level: str = 'INFO'):
    """
    Crea y retorna un logger configurado
    
    Args:
        name: Nombre del módulo
        log_dir: Directorio de logs (opcional)
        level: Nivel de logging
    
    Returns:
        logging.Logger configurado
    """
    if log_dir is None:
        from config.config import Config
        log_dir = Config.ETL_LOG_PATH
    
    etl_logger = ETLLogger(name, log_dir, level)
    return etl_logger.get_logger()