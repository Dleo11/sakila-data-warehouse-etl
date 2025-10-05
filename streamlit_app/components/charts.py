"""
Funciones auxiliares para crear gráficos
"""
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

def create_line_chart(df: pd.DataFrame, x: str, y: str, color: str = None, title: str = ""):
    """Crea un gráfico de líneas"""
    fig = px.line(
        df, x=x, y=y, color=color,
        title=title,
        markers=True
    )
    fig.update_layout(
        hovermode='x unified',
        showlegend=True,
        height=400
    )
    return fig

def create_bar_chart(df: pd.DataFrame, x: str, y: str, color: str = None, 
                     title: str = "", orientation: str = 'v'):
    """Crea un gráfico de barras"""
    fig = px.bar(
        df, x=x, y=y, color=color,
        title=title,
        orientation=orientation
    )
    fig.update_layout(height=400)
    return fig

def create_pie_chart(df: pd.DataFrame, values: str, names: str, title: str = "", hole: float = 0):
    """Crea un gráfico de pastel/dona"""
    fig = px.pie(
        df, values=values, names=names,
        title=title,
        hole=hole
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

def create_scatter_chart(df: pd.DataFrame, x: str, y: str, color: str = None,
                        size: str = None, title: str = ""):
    """Crea un gráfico de dispersión"""
    fig = px.scatter(
        df, x=x, y=y, color=color, size=size,
        title=title
    )
    fig.update_layout(height=400)
    return fig

def create_heatmap(df: pd.DataFrame, x: str, y: str, z: str, title: str = ""):
    """Crea un mapa de calor"""
    pivot_table = df.pivot_table(values=z, index=y, columns=x, aggfunc='sum')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_table.values,
        x=pivot_table.columns,
        y=pivot_table.index,
        colorscale='RdYlGn'
    ))
    
    fig.update_layout(
        title=title,
        height=500
    )
    return fig

def create_comparison_chart(df: pd.DataFrame, categories: list, metrics: dict, title: str = ""):
    """
    Crea un gráfico de comparación múltiple
    
    Args:
        df: DataFrame
        categories: Lista de categorías (eje X)
        metrics: Dict de métricas {nombre: columna}
        title: Título
    """
    fig = go.Figure()
    
    for name, column in metrics.items():
        fig.add_trace(go.Bar(
            name=name,
            x=categories,
            y=df[column],
            text=df[column],
            textposition='auto'
        ))
    
    fig.update_layout(
        title=title,
        barmode='group',
        height=400
    )
    return fig