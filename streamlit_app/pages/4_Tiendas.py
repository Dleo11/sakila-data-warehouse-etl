"""
Página de Análisis de Tiendas
Comparativa y análisis detallado por tienda
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
import plotly.graph_objects as go

# Agregar ruta para imports
root_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_path))

from streamlit_app.utils.db_connection import db
from streamlit_app.utils.queries import Queries
from streamlit_app.components.kpi_cards import kpi_row, highlight_metric
from streamlit_app.components.charts import create_bar_chart, create_line_chart

# Configuración de página
st.set_page_config(
    page_title="Tiendas - Sakila Dashboard",
    page_icon="🏪",
    layout="wide"
)

# Header
st.title("🏪 Análisis de Tiendas")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("## 🎬 Sakila Dashboard")
    st.markdown("### Análisis de Tiendas")
    st.markdown("---")
    
    st.markdown("### 🔍 Opciones")
    
    # Selector de vista
    vista = st.radio(
        "Tipo de vista:",
        ["Comparativa", "Individual"]
    )
    
    st.markdown("---")
    if st.button("🔄 Refrescar Datos"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    st.info("""
    **💡 Sobre las Tiendas:**
    
    Sakila cuenta con 2 tiendas ubicadas en diferentes ciudades.
    
    Este análisis permite comparar el rendimiento y identificar 
    oportunidades de mejora.
    """)

# Cargar datos
with st.spinner("🏪 Cargando datos de tiendas..."):
    df_tiendas = db.execute_query(Queries.VENTAS_POR_TIENDA)
    df_ventas_mensuales = db.execute_query(Queries.VENTAS_MENSUALES_TIENDA)

# KPIs Principales
st.markdown("## 📊 Métricas Generales")

if not df_tiendas.empty:
    total_tiendas = len(df_tiendas)
    total_rentas = df_tiendas['total_rentas'].sum()
    total_ventas = df_tiendas['total_ventas'].sum()
    ticket_promedio = df_tiendas['ticket_promedio'].mean()
    dias_promedio = df_tiendas['dias_promedio'].mean()
    
    kpi_row([
        {"label": "🏪 Total Tiendas", "value": f"{total_tiendas}"},
        {"label": "📦 Total Rentas", "value": f"{int(total_rentas):,}"},
        {"label": "💰 Total Ventas", "value": f"${total_ventas:,.2f}"},
        {"label": "🎟️ Ticket Prom.", "value": f"${ticket_promedio:.2f}"},
        {"label": "⏱️ Días Prom.", "value": f"{dias_promedio:.1f} días"}
    ])

st.markdown("---")

# Vista Comparativa
if vista == "Comparativa":
    st.markdown("## 📊 Comparativa entre Tiendas")
    
    if len(df_tiendas) >= 2:
        col1, col2 = st.columns(2)
        
        # Preparar datos para gráficos comparativos
        tienda_1 = df_tiendas.iloc[0]
        tienda_2 = df_tiendas.iloc[1]
        
        with col1:
            st.markdown(f"### 🏪 {tienda_1['nombre_tienda']}")
            st.markdown(f"**📍 Ubicación:** {tienda_1['ciudad']}, {tienda_1['pais']}")
            
            kpi_row([
                {"label": "📦 Rentas", "value": f"{int(tienda_1['total_rentas']):,}"},
                {"label": "💰 Ventas", "value": f"${tienda_1['total_ventas']:,.2f}"}
            ])
            
            kpi_row([
                {"label": "🎟️ Ticket", "value": f"${tienda_1['ticket_promedio']:.2f}"},
                {"label": "⏱️ Días", "value": f"{tienda_1['dias_promedio']:.1f}"}
            ])
        
        with col2:
            st.markdown(f"### 🏪 {tienda_2['nombre_tienda']}")
            st.markdown(f"**📍 Ubicación:** {tienda_2['ciudad']}, {tienda_2['pais']}")
            
            kpi_row([
                {"label": "📦 Rentas", "value": f"{int(tienda_2['total_rentas']):,}"},
                {"label": "💰 Ventas", "value": f"${tienda_2['total_ventas']:,.2f}"}
            ])
            
            kpi_row([
                {"label": "🎟️ Ticket", "value": f"${tienda_2['ticket_promedio']:.2f}"},
                {"label": "⏱️ Días", "value": f"{tienda_2['dias_promedio']:.1f}"}
            ])
        
        st.markdown("---")
        
        # Gráficos comparativos
        st.markdown("### 📈 Comparativa Visual")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de barras comparativo
            fig_comparativa = go.Figure()
            
            metricas = ['Rentas', 'Ventas ($)', 'Ticket Prom. ($)']
            valores_t1 = [tienda_1['total_rentas'], tienda_1['total_ventas'], tienda_1['ticket_promedio']]
            valores_t2 = [tienda_2['total_rentas'], tienda_2['total_ventas'], tienda_2['ticket_promedio']]
            
            fig_comparativa.add_trace(go.Bar(
                name=tienda_1['nombre_tienda'],
                x=metricas,
                y=valores_t1,
                text=[f"{v:,.0f}" if i < 2 else f"${v:.2f}" for i, v in enumerate(valores_t1)],
                textposition='auto',
                marker_color='#FF4B4B'
            ))
            
            fig_comparativa.add_trace(go.Bar(
                name=tienda_2['nombre_tienda'],
                x=metricas,
                y=valores_t2,
                text=[f"{v:,.0f}" if i < 2 else f"${v:.2f}" for i, v in enumerate(valores_t2)],
                textposition='auto',
                marker_color='#4CAF50'
            ))
            
            fig_comparativa.update_layout(
                title='Comparativa de Métricas Clave',
                barmode='group',
                height=400
            )
            
            st.plotly_chart(fig_comparativa, use_container_width=True)
        
        with col2:
            # Gráfico de radar
            fig_radar = go.Figure()
            
            # Normalizar valores para el radar
            max_rentas = max(tienda_1['total_rentas'], tienda_2['total_rentas'])
            max_ventas = max(tienda_1['total_ventas'], tienda_2['total_ventas'])
            max_ticket = max(tienda_1['ticket_promedio'], tienda_2['ticket_promedio'])
            max_dias = max(tienda_1['dias_promedio'], tienda_2['dias_promedio'])
            
            categorias_radar = ['Rentas', 'Ventas', 'Ticket', 'Días Renta']
            
            valores_norm_t1 = [
                (tienda_1['total_rentas'] / max_rentas) * 100,
                (tienda_1['total_ventas'] / max_ventas) * 100,
                (tienda_1['ticket_promedio'] / max_ticket) * 100,
                (tienda_1['dias_promedio'] / max_dias) * 100
            ]
            
            valores_norm_t2 = [
                (tienda_2['total_rentas'] / max_rentas) * 100,
                (tienda_2['total_ventas'] / max_ventas) * 100,
                (tienda_2['ticket_promedio'] / max_ticket) * 100,
                (tienda_2['dias_promedio'] / max_dias) * 100
            ]
            
            fig_radar.add_trace(go.Scatterpolar(
                r=valores_norm_t1,
                theta=categorias_radar,
                fill='toself',
                name=tienda_1['nombre_tienda'],
                line_color='#FF4B4B'
            ))
            
            fig_radar.add_trace(go.Scatterpolar(
                r=valores_norm_t2,
                theta=categorias_radar,
                fill='toself',
                name=tienda_2['nombre_tienda'],
                line_color='#4CAF50'
            ))
            
            fig_radar.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                title='Comparativa Normalizada (Radar)',
                height=400
            )
            
            st.plotly_chart(fig_radar, use_container_width=True)
        
        # Evolución temporal
        st.markdown("### 📈 Evolución Temporal")
        
        if not df_ventas_mensuales.empty:
            # Preparar datos
            df_ventas_mensuales['periodo'] = (
                df_ventas_mensuales['anio'].astype(str) + '-' + 
                df_ventas_mensuales['mes'].astype(str).str.zfill(2)
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Evolución de ventas
                fig_evolucion_ventas = create_line_chart(
                    df_ventas_mensuales,
                    x='periodo',
                    y='ingresos_totales',
                    color='nombre_tienda',
                    title='💰 Evolución de Ventas Mensuales'
                )
                st.plotly_chart(fig_evolucion_ventas, use_container_width=True)
            
            with col2:
                # Evolución de rentas
                fig_evolucion_rentas = create_line_chart(
                    df_ventas_mensuales,
                    x='periodo',
                    y='total_rentas',
                    color='nombre_tienda',
                    title='📦 Evolución de Rentas Mensuales'
                )
                st.plotly_chart(fig_evolucion_rentas, use_container_width=True)
        
        # Análisis de brechas
        st.markdown("### 🔍 Análisis de Diferencias")
        
        col1, col2, col3 = st.columns(3)
        
        diff_rentas = tienda_1['total_rentas'] - tienda_2['total_rentas']
        diff_ventas = tienda_1['total_ventas'] - tienda_2['total_ventas']
        diff_ticket = tienda_1['ticket_promedio'] - tienda_2['ticket_promedio']
        
        with col1:
            if diff_rentas > 0:
                st.success(f"✅ **{tienda_1['nombre_tienda']}** lidera en rentas por **{int(abs(diff_rentas)):,}** unidades")
            else:
                st.success(f"✅ **{tienda_2['nombre_tienda']}** lidera en rentas por **{int(abs(diff_rentas)):,}** unidades")
        
        with col2:
            if diff_ventas > 0:
                st.success(f"✅ **{tienda_1['nombre_tienda']}** lidera en ventas por **${abs(diff_ventas):,.2f}**")
            else:
                st.success(f"✅ **{tienda_2['nombre_tienda']}** lidera en ventas por **${abs(diff_ventas):,.2f}**")
        
        with col3:
            if abs(diff_ticket) < 0.5:
                st.info("⚖️ Ticket promedio muy similar entre tiendas")
            elif diff_ticket > 0:
                st.info(f"📊 **{tienda_1['nombre_tienda']}** tiene mayor ticket (+${abs(diff_ticket):.2f})")
            else:
                st.info(f"📊 **{tienda_2['nombre_tienda']}** tiene mayor ticket (+${abs(diff_ticket):.2f})")

# Vista Individual
else:
    st.markdown("## 🏪 Vista Individual por Tienda")
    
    if not df_tiendas.empty:
        # Selector de tienda
        tiendas_lista = df_tiendas['nombre_tienda'].tolist()
        tienda_seleccionada = st.selectbox("Seleccionar tienda:", tiendas_lista)
        
        # Filtrar datos de la tienda seleccionada
        tienda_data = df_tiendas[df_tiendas['nombre_tienda'] == tienda_seleccionada].iloc[0]
        
        st.markdown(f"### 📍 {tienda_data['nombre_tienda']}")
        st.markdown(f"**Ubicación:** {tienda_data['ciudad']}, {tienda_data['pais']}")
        st.markdown("---")
        
        # KPIs de la tienda
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📦 Total Rentas", f"{int(tienda_data['total_rentas']):,}")
        with col2:
            st.metric("💰 Total Ventas", f"${tienda_data['total_ventas']:,.2f}")
        with col3:
            st.metric("🎟️ Ticket Promedio", f"${tienda_data['ticket_promedio']:.2f}")
        with col4:
            st.metric("⏱️ Días Promedio", f"{tienda_data['dias_promedio']:.1f}")
        
        st.markdown("---")
        
        # Evolución de la tienda
        if not df_ventas_mensuales.empty:
            df_tienda_mensual = df_ventas_mensuales[
                df_ventas_mensuales['nombre_tienda'] == tienda_seleccionada
            ]
            
            if not df_tienda_mensual.empty:
                df_tienda_mensual['periodo'] = (
                    df_tienda_mensual['anio'].astype(str) + '-' + 
                    df_tienda_mensual['mes'].astype(str).str.zfill(2)
                )
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Gráfico de área - Ventas
                    import plotly.express as px
                    fig_area_ventas = px.area(
                        df_tienda_mensual,
                        x='periodo',
                        y='ingresos_totales',
                        title=f'💰 Evolución de Ventas - {tienda_seleccionada}',
                        labels={'periodo': 'Período', 'ingresos_totales': 'Ventas ($)'}
                    )
                    fig_area_ventas.update_traces(line_color='#4CAF50', fillcolor='rgba(76, 175, 80, 0.3)')
                    st.plotly_chart(fig_area_ventas, use_container_width=True)
                
                with col2:
                    # Gráfico de barras - Rentas
                    fig_bar_rentas = create_bar_chart(
                        df_tienda_mensual,
                        x='periodo',
                        y='total_rentas',
                        color='total_rentas',
                        title=f'📦 Rentas Mensuales - {tienda_seleccionada}'
                    )
                    st.plotly_chart(fig_bar_rentas, use_container_width=True)
                
                # Tabla de datos mensuales
                st.markdown("#### 📋 Detalle Mensual")
                df_display = df_tienda_mensual[['periodo', 'mes_nombre', 'total_rentas', 'ingresos_totales', 'ticket_promedio']].copy()
                df_display['ingresos_totales'] = df_display['ingresos_totales'].apply(lambda x: f"${x:,.2f}")
                df_display['ticket_promedio'] = df_display['ticket_promedio'].apply(lambda x: f"${x:.2f}")
                
                st.dataframe(df_display, use_container_width=True, hide_index=True)

# Tabla completa de datos
st.markdown("---")
st.markdown("## 📋 Datos Completos")

if not df_tiendas.empty:
    df_tiendas_display = df_tiendas.copy()
    df_tiendas_display['total_ventas'] = df_tiendas_display['total_ventas'].apply(lambda x: f"${x:,.2f}")
    df_tiendas_display['ticket_promedio'] = df_tiendas_display['ticket_promedio'].apply(lambda x: f"${x:.2f}")
    df_tiendas_display['dias_promedio'] = df_tiendas_display['dias_promedio'].apply(lambda x: f"{x:.1f}")
    
    st.dataframe(df_tiendas_display, use_container_width=True, hide_index=True)
    
    # Botón de descarga
    csv = df_tiendas.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Descargar Datos de Tiendas",
        data=csv,
        file_name="tiendas_analisis.csv",
        mime="text/csv"
    )

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888;'>
        <p>🏪 Análisis de Tiendas | Sakila Data Warehouse</p>
    </div>
""", unsafe_allow_html=True)