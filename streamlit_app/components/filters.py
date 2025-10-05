"""
Componentes de filtros reutilizables
"""
import streamlit as st
from datetime import datetime, timedelta
import pandas as pd

def date_range_filter(df: pd.DataFrame, date_column: str = 'fecha'):
    """
    Crea filtros de rango de fechas
    
    Args:
        df: DataFrame con columna de fecha
        date_column: Nombre de la columna de fecha
    
    Returns:
        Tupla (fecha_inicio, fecha_fin)
    """
    if df.empty or date_column not in df.columns:
        return None, None
    
    min_date = df[date_column].min()
    max_date = df[date_column].max()
    
    st.markdown("#### 📅 Rango de Fechas")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fecha_inicio = st.date_input(
            "Fecha Inicio",
            value=min_date,
            min_value=min_date,
            max_value=max_date
        )
    
    with col2:
        fecha_fin = st.date_input(
            "Fecha Fin",
            value=max_date,
            min_value=min_date,
            max_value=max_date
        )
    
    return fecha_inicio, fecha_fin

def multiselect_filter(options: list, label: str, default: list = None, key: str = None):
    """
    Crea un filtro multiselect
    
    Args:
        options: Lista de opciones
        label: Etiqueta del filtro
        default: Opciones por defecto
        key: Key única para el widget
    
    Returns:
        Lista de valores seleccionados
    """
    if default is None:
        default = options
    
    selected = st.multiselect(
        label,
        options=options,
        default=default,
        key=key
    )
    
    return selected

def slider_filter(min_val: float, max_val: float, label: str, step: float = 1.0, key: str = None):
    """
    Crea un filtro slider de rango
    
    Args:
        min_val: Valor mínimo
        max_val: Valor máximo
        label: Etiqueta
        step: Paso del slider
        key: Key única
    
    Returns:
        Tupla (valor_min, valor_max)
    """
    values = st.slider(
        label,
        min_value=min_val,
        max_value=max_val,
        value=(min_val, max_val),
        step=step,
        key=key
    )
    
    return values

def search_box(label: str = "🔍 Buscar", placeholder: str = "Escribe para buscar...", key: str = None):
    """
    Crea una caja de búsqueda
    
    Args:
        label: Etiqueta
        placeholder: Texto placeholder
        key: Key única
    
    Returns:
        Texto ingresado
    """
    search_term = st.text_input(
        label,
        placeholder=placeholder,
        key=key
    )
    
    return search_term

def create_sidebar_filters(db):
    """
    Crea filtros completos en el sidebar
    
    Args:
        db: Instancia de DatabaseConnection
    
    Returns:
        Diccionario con filtros seleccionados
    """
    st.sidebar.markdown("## 🔍 Filtros")
    
    # Obtener datos para filtros
    from utils.queries import Queries
    
    df_fechas = db.execute_query(Queries.GET_FECHAS_DISPONIBLES)
    df_categorias = db.execute_query(Queries.GET_CATEGORIAS)
    df_tiendas = db.execute_query(Queries.GET_TIENDAS)
    
    filters = {}
    
    # Filtro de fechas
    if not df_fechas.empty:
        fecha_min = pd.to_datetime(df_fechas['fecha_min'].iloc[0])
        fecha_max = pd.to_datetime(df_fechas['fecha_max'].iloc[0])
        
        st.sidebar.markdown("### 📅 Período")
        filters['fecha_inicio'] = st.sidebar.date_input(
            "Desde",
            value=fecha_min,
            min_value=fecha_min,
            max_value=fecha_max
        )
        filters['fecha_fin'] = st.sidebar.date_input(
            "Hasta",
            value=fecha_max,
            min_value=fecha_min,
            max_value=fecha_max
        )
    
    # Filtro de categorías
    if not df_categorias.empty:
        st.sidebar.markdown("### 📂 Categorías")
        categorias = df_categorias['nombre_categoria'].tolist()
        filters['categorias'] = st.sidebar.multiselect(
            "Seleccionar categorías",
            options=categorias,
            default=categorias
        )
    
    # Filtro de tiendas
    if not df_tiendas.empty:
        st.sidebar.markdown("### 🏪 Tiendas")
        tiendas = df_tiendas['nombre_tienda'].tolist()
        filters['tiendas'] = st.sidebar.multiselect(
            "Seleccionar tiendas",
            options=tiendas,
            default=tiendas
        )
    
    return filters