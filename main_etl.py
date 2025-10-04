"""
Script Orquestador Principal del ETL Sakila
RF10: Orquestación del proceso completo

Ejecuta el flujo ETL completo:
1. Extracción desde Sakila
2. Validaciones PRE-limpieza
3. Limpieza y transformaciones en Staging
4. Validaciones POST-limpieza
5. Transformación a modelo estrella (Data Mart)
6. Reporte final de ejecución

Uso:
    python main_etl.py [--incremental] [--skip-validation] [--force]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# Agregar path del proyecto
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from config.config import Config
from src.extractor import SakilaExtractor
from src.validator import DataValidator
from src.staging import StagingProcessor
from src.transformer import DataMartTransformer
from src.logger_config import ETLLogger

class ETLOrchestrator:
    """Orquestador del proceso ETL completo"""
    
    def __init__(self, incremental: bool = False, skip_validation: bool = False):
        """
        Inicializa el orquestador
        
        Args:
            incremental: Si True, ejecuta extracción incremental
            skip_validation: Si True, omite validaciones (no recomendado)
        """
        self.incremental = incremental
        self.skip_validation = skip_validation
        
        # Logger principal
        self.etl_logger = ETLLogger('orchestrator', Config.ETL_LOG_PATH, Config.ETL_LOG_LEVEL)
        self.logger = self.etl_logger.get_logger()
        
        # ID de ejecución
        self.etl_id: Optional[int] = None
        
        # Estadísticas
        self.stats = {
            'inicio': None,
            'fin': None,
            'duracion_total': None,
            'fase_actual': None,
            'exito': False,
            'error': None,
            'extraccion': {},
            'validacion_pre': {},
            'limpieza': {},
            'validacion_post': {},
            'transformacion': {}
        }
    
    def ejecutar(self) -> bool:
        """
        Ejecuta el proceso ETL completo
        
        Returns:
            True si exitoso, False si error
        """
        self.stats['inicio'] = datetime.now()
        
        self.logger.info("="*80)
        self.logger.info("🚀 INICIANDO PROCESO ETL COMPLETO")
        self.logger.info("="*80)
        self.logger.info(f"Modo: {'INCREMENTAL' if self.incremental else 'COMPLETO'}")
        self.logger.info(f"Validaciones: {'OMITIDAS' if self.skip_validation else 'ACTIVADAS'}")
        self.logger.info("="*80)
        
        try:
            # FASE 1: Extracción
            if not self._fase_extraccion():
                return False
            
            # FASE 2: Validación PRE
            if not self.skip_validation and not self._fase_validacion_pre():
                return False
            
            # FASE 3: Limpieza
            if not self._fase_limpieza():
                return False
            
            # FASE 4: Validación POST
            if not self.skip_validation and not self._fase_validacion_post():
                return False
            
            # FASE 5: Transformación a Data Mart
            if not self._fase_transformacion():
                return False
            
            # Éxito
            self.stats['exito'] = True
            self._generar_reporte_final()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ ERROR CRÍTICO EN ETL: {e}")
            self.stats['error'] = str(e)
            self.stats['exito'] = False
            self._generar_reporte_final()
            return False
        
        finally:
            self.stats['fin'] = datetime.now()
            if self.stats['inicio']:
                self.stats['duracion_total'] = (self.stats['fin'] - self.stats['inicio']).total_seconds()
    
    def _fase_extraccion(self) -> bool:
        """Fase 1: Extracción de datos"""
        self.stats['fase_actual'] = 'EXTRACCION'
        
        self.etl_logger.log_etl_start("FASE 1: EXTRACCION", 
                                     f"Extrayendo datos de Sakila ({'incremental' if self.incremental else 'completo'})")
        
        try:
            extractor = SakilaExtractor()
            
            # Determinar fecha desde para incremental
            fecha_desde = None
            if self.incremental:
                fecha_desde = extractor.obtener_ultima_extraccion()
                if not fecha_desde:
                    self.logger.warning("⚠️  No hay extracción previa, cambiando a modo COMPLETO")
                    self.incremental = False
            
            # Ejecutar extracción
            stats = extractor.extraer_todas_las_tablas(
                incremental=self.incremental,
                fecha_desde=fecha_desde
            )
            
            # Guardar ETL ID
            self.etl_id = extractor.etl_id
            
            # Guardar estadísticas
            self.stats['extraccion'] = stats
            
            # Calcular totales
            total_leidos = sum(s['leidos'] for s in stats.values() if 'leidos' in s)
            total_errores = sum(1 for s in stats.values() if 'error' in s)
            
            extractor.cerrar_conexiones()
            
            self.etl_logger.log_etl_end("FASE 1: EXTRACCION", exito=True, detalles={
                'Total registros': f"{total_leidos:,}",
                'Tablas procesadas': len(stats),
                'Errores': total_errores
            })
            
            if total_errores > 0:
                self.logger.warning(f"⚠️  Extracción completada con {total_errores} errores")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en fase de extracción: {e}")
            self.etl_logger.log_etl_end("FASE 1: EXTRACCION", exito=False)
            return False
    
    def _fase_validacion_pre(self) -> bool:
        """Fase 2: Validaciones PRE-limpieza"""
        self.stats['fase_actual'] = 'VALIDACION_PRE'
        
        self.etl_logger.log_etl_start("FASE 2: VALIDACION PRE", 
                                     "Validando calidad de datos crudos")
        
        try:
            validator = DataValidator(etl_id=self.etl_id)
            
            resultados = validator.ejecutar_validaciones_staging()
            
            self.stats['validacion_pre'] = resultados
            
            total_validaciones = len(resultados)
            exitosas = sum(resultados.values())
            tasa_exito = (exitosas / total_validaciones * 100) if total_validaciones > 0 else 0
            
            validator.cerrar_conexion()
            
            self.etl_logger.log_etl_end("FASE 2: VALIDACION PRE", exito=True, detalles={
                'Total validaciones': total_validaciones,
                'Exitosas': exitosas,
                'Fallidas': total_validaciones - exitosas,
                'Tasa de éxito': f"{tasa_exito:.1f}%"
            })
            
            # Advertir si tasa de éxito es muy baja
            if tasa_exito < 50:
                self.logger.warning(f"⚠️  ADVERTENCIA: Tasa de éxito baja ({tasa_exito:.1f}%)")
                self.logger.warning("   Considerar revisar los datos de origen")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en validación PRE: {e}")
            self.etl_logger.log_etl_end("FASE 2: VALIDACION PRE", exito=False)
            return False
    
    def _fase_limpieza(self) -> bool:
        """Fase 3: Limpieza y transformaciones en staging"""
        self.stats['fase_actual'] = 'LIMPIEZA'
        
        self.etl_logger.log_etl_start("FASE 3: LIMPIEZA", 
                                     "Procesando y limpiando datos en staging")
        
        try:
            processor = StagingProcessor(etl_id=self.etl_id)
            
            stats = processor.procesar_todas_las_tablas()
            
            self.stats['limpieza'] = stats
            
            # Calcular totales
            total_duplicados = sum(s.get('duplicados', 0) for s in stats.values())
            total_invalidos = sum(
                sum(v for k, v in s.items() if 'invalido' in k or 'negativo' in k)
                for s in stats.values()
            )
            
            processor.cerrar_conexion()
            
            self.etl_logger.log_etl_end("FASE 3: LIMPIEZA", exito=True, detalles={
                'Tablas procesadas': len(stats),
                'Duplicados eliminados': total_duplicados,
                'Registros inválidos': total_invalidos
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en limpieza: {e}")
            self.etl_logger.log_etl_end("FASE 3: LIMPIEZA", exito=False)
            return False
    
    def _fase_validacion_post(self) -> bool:
        """Fase 4: Validaciones POST-limpieza"""
        self.stats['fase_actual'] = 'VALIDACION_POST'
        
        self.etl_logger.log_etl_start("FASE 4: VALIDACION POST", 
                                     "Validando datos después de limpieza")
        
        try:
            validator = DataValidator(etl_id=self.etl_id)
            
            resultados = validator.ejecutar_validaciones_staging()
            
            self.stats['validacion_post'] = resultados
            
            total_validaciones = len(resultados)
            exitosas = sum(resultados.values())
            tasa_exito = (exitosas / total_validaciones * 100) if total_validaciones > 0 else 0
            
            # Calcular mejora
            if self.stats['validacion_pre']:
                tasa_pre = (sum(self.stats['validacion_pre'].values()) / 
                           len(self.stats['validacion_pre']) * 100)
                mejora = tasa_exito - tasa_pre
            else:
                mejora = 0
            
            validator.cerrar_conexion()
            
            self.etl_logger.log_etl_end("FASE 4: VALIDACION POST", exito=True, detalles={
                'Total validaciones': total_validaciones,
                'Exitosas': exitosas,
                'Tasa de éxito': f"{tasa_exito:.1f}%",
                'Mejora vs PRE': f"{mejora:+.1f}%"
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en validación POST: {e}")
            self.etl_logger.log_etl_end("FASE 4: VALIDACION POST", exito=False)
            return False
    
    def _fase_transformacion(self) -> bool:
        """Fase 5: Transformación a Data Mart"""
        self.stats['fase_actual'] = 'TRANSFORMACION'
        
        self.etl_logger.log_etl_start("FASE 5: TRANSFORMACION", 
                                     "Cargando datos al modelo estrella")
        
        try:
            transformer = DataMartTransformer(etl_id=self.etl_id)
            
            stats = transformer.ejecutar_transformacion_completa()
            
            self.stats['transformacion'] = stats
            
            transformer.cerrar_conexiones()
            
            self.etl_logger.log_etl_end("FASE 5: TRANSFORMACION", exito=True, detalles={
                'Dimensiones pobladas': 4,
                'Hechos cargados': f"{stats.get('fact_ventas', 0):,}"
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en transformación: {e}")
            self.etl_logger.log_etl_end("FASE 5: TRANSFORMACION", exito=False)
            return False
    
    def _generar_reporte_final(self):
        """Genera reporte final de ejecución"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("📊 REPORTE FINAL DE EJECUCIÓN ETL")
        self.logger.info("="*80)
        
        # Estado general
        status = "✅ EXITOSO" if self.stats['exito'] else "❌ FALLIDO"
        self.logger.info(f"Estado: {status}")
        
        if self.stats['error']:
            self.logger.error(f"Error: {self.stats['error']}")
        
        # Tiempos
        if self.stats['duracion_total']:
            self.logger.info(f"Duración total: {self.stats['duracion_total']:.1f} segundos")
            self.logger.info(f"Inicio: {self.stats['inicio'].strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"Fin: {self.stats['fin'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Estadísticas por fase
        self.logger.info("")
        self.logger.info("Estadísticas por fase:")
        
        # Extracción
        if self.stats['extraccion']:
            total = sum(s['leidos'] for s in self.stats['extraccion'].values() if 'leidos' in s)
            self.logger.info(f"  1. Extracción: {total:,} registros")
        
        # Validación PRE
        if self.stats['validacion_pre']:
            exitosas = sum(self.stats['validacion_pre'].values())
            total = len(self.stats['validacion_pre'])
            tasa = (exitosas / total * 100) if total > 0 else 0
            self.logger.info(f"  2. Validación PRE: {exitosas}/{total} ({tasa:.1f}%)")
        
        # Limpieza
        if self.stats['limpieza']:
            duplicados = sum(s.get('duplicados', 0) for s in self.stats['limpieza'].values())
            self.logger.info(f"  3. Limpieza: {duplicados} duplicados eliminados")
        
        # Validación POST
        if self.stats['validacion_post']:
            exitosas = sum(self.stats['validacion_post'].values())
            total = len(self.stats['validacion_post'])
            tasa = (exitosas / total * 100) if total > 0 else 0
            self.logger.info(f"  4. Validación POST: {exitosas}/{total} ({tasa:.1f}%)")
        
        # Transformación
        if self.stats['transformacion']:
            fact = self.stats['transformacion'].get('fact_ventas', 0)
            self.logger.info(f"  5. Transformación: {fact:,} registros en fact_ventas")
        
        self.logger.info("="*80)
        
        if self.stats['exito']:
            self.logger.info("🎉 ¡ETL COMPLETADO EXITOSAMENTE!")
        else:
            self.logger.error("💥 ETL FALLÓ - Revisar logs para detalles")
        
        self.logger.info("="*80)

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Orquestador ETL Sakila → Data Mart',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python main_etl.py                    # Extracción completa
  python main_etl.py --incremental      # Extracción incremental
  python main_etl.py --skip-validation  # Omitir validaciones (no recomendado)
  python main_etl.py --force            # Forzar ejecución sin confirmación
        """
    )
    
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Ejecutar extracción incremental (solo datos nuevos)'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Omitir validaciones de calidad (no recomendado)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Forzar ejecución sin confirmación'
    )
    
    args = parser.parse_args()
    
    # Confirmación antes de ejecutar
    if not args.force:
        print("\n" + "="*80)
        print("ORQUESTADOR ETL - SAKILA DATA WAREHOUSE")
        print("="*80)
        print(f"Modo: {'INCREMENTAL' if args.incremental else 'COMPLETO'}")
        print(f"Validaciones: {'OMITIDAS' if args.skip_validation else 'ACTIVADAS'}")
        print("="*80)
        
        respuesta = input("\n¿Desea continuar? (s/n): ").strip().lower()
        if respuesta not in ['s', 'si', 'y', 'yes']:
            print("Ejecución cancelada por el usuario")
            return 1
    
    # Ejecutar ETL
    orchestrator = ETLOrchestrator(
        incremental=args.incremental,
        skip_validation=args.skip_validation
    )
    
    exito = orchestrator.ejecutar()
    
    # Código de salida
    return 0 if exito else 1

if __name__ == '__main__':
    sys.exit(main())