"""
Componentes de tarjetas KPI reutilizables
"""
import streamlit as st
from typing import Optional

def metric_card(label: str, value: str, delta: Optional[str] = None, delta_color: str = "normal"):
    """
    Crea una tarjeta de métrica estilizada
    
    Args:
        label: Etiqueta del KPI
        value: Valor a mostrar
        delta: Cambio o subtítulo (opcional)
        delta_color: Color del delta ("normal", "inverse", "off")
    """
    st.metric(
        label=label,
        value=value,
        delta=delta,
        delta_color=delta_color
    )

def kpi_row(metrics: list):
    """
    Crea una fila de KPIs
    
    Args:
        metrics: Lista de diccionarios con keys: label, value, delta (opcional)
    
    Example:
        kpi_row([
            {"label": "Total Ventas", "value": "$1,000", "delta": "+10%"},
            {"label": "Total Rentas", "value": "500", "delta": "+5%"}
        ])
    """
    cols = st.columns(len(metrics))
    
    for col, metric in zip(cols, metrics):
        with col:
            metric_card(
                label=metric.get('label', ''),
                value=metric.get('value', ''),
                delta=metric.get('delta', None)
            )

def summary_box(title: str, metrics: dict, icon: str = "📊"):
    """
    Crea una caja de resumen con múltiples métricas
    
    Args:
        title: Título de la caja
        metrics: Diccionario de métricas {label: value}
        icon: Emoji del icono
    """
    st.markdown(f"### {icon} {title}")
    
    # Crear columnas dinámicamente
    num_metrics = len(metrics)
    cols = st.columns(num_metrics)
    
    for col, (label, value) in zip(cols, metrics.items()):
        with col:
            st.metric(label=label, value=value)

def info_card(title: str, content: str, type: str = "info"):
    """
    Crea una tarjeta informativa
    
    Args:
        title: Título de la tarjeta
        content: Contenido de la tarjeta
        type: Tipo de alerta ("info", "success", "warning", "error")
    """
    if type == "info":
        st.info(f"**{title}**\n\n{content}")
    elif type == "success":
        st.success(f"**{title}**\n\n{content}")
    elif type == "warning":
        st.warning(f"**{title}**\n\n{content}")
    elif type == "error":
        st.error(f"**{title}**\n\n{content}")

def highlight_metric(label: str, value: str, comparison: str = "", color: str = "#FF4B4B"):
    """
    Crea una métrica destacada con estilo personalizado
    
    Args:
        label: Etiqueta
        value: Valor principal
        comparison: Texto de comparación
        color: Color del valor
    """
    st.markdown(f"""
        <div style='
            background-color: #262730;
            padding: 20px;
            border-radius: 10px;
            border-left: 5px solid {color};
            margin: 10px 0;
        '>
            <p style='color: #888; margin: 0; font-size: 14px;'>{label}</p>
            <h2 style='color: {color}; margin: 5px 0;'>{value}</h2>
            <p style='color: #AAA; margin: 0; font-size: 12px;'>{comparison}</p>
        </div>
    """, unsafe_allow_html=True)