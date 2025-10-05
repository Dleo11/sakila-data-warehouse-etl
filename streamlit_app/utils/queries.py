"""
Queries SQL predefinidas para el dashboard
Aprovecha las vistas analíticas ya creadas en sakila_dw
"""

class Queries:
    """Colección de queries SQL para el dashboard"""
    
    # ========== KPIs PRINCIPALES ==========
    
    RESUMEN_EJECUTIVO = """
        SELECT * FROM v_resumen_ejecutivo
    """
    
    KPI_TOTALES = """
        SELECT 
            SUM(cantidad_rentas) as total_rentas,
            SUM(monto_total) as total_ventas,
            AVG(monto_promedio) as ticket_promedio,
            AVG(dias_renta_promedio) as dias_promedio_renta,
            COUNT(DISTINCT film_sk) as total_peliculas_rentadas,
            COUNT(DISTINCT tienda_sk) as total_tiendas
        FROM fact_ventas
    """
    
    # ========== VENTAS ==========
    
    VENTAS_MENSUALES_TIENDA = """
        SELECT * FROM v_ventas_mensuales_tienda
        ORDER BY anio, mes, nombre_tienda
    """
    
    VENTAS_POR_PERIODO = """
        SELECT 
            dt.anio,
            dt.mes,
            dt.mes_nombre,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as total_ventas,
            AVG(fv.monto_promedio) as ticket_promedio
        FROM fact_ventas fv
        JOIN dim_tiempo dt ON fv.fecha_id = dt.fecha_id
        GROUP BY dt.anio, dt.mes, dt.mes_nombre
        ORDER BY dt.anio, dt.mes
    """
    
    VENTAS_POR_TIENDA = """
        SELECT 
            ds.tienda_sk,
            ds.nombre_tienda,
            ds.ciudad,
            ds.pais,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as total_ventas,
            AVG(fv.monto_promedio) as ticket_promedio,
            AVG(fv.dias_renta_promedio) as dias_promedio
        FROM fact_ventas fv
        JOIN dim_tienda ds ON fv.tienda_sk = ds.tienda_sk AND ds.activo = TRUE
        GROUP BY ds.tienda_sk, ds.nombre_tienda, ds.ciudad, ds.pais
        ORDER BY total_ventas DESC
    """
    
    # ========== PELÍCULAS ==========
    
    TOP_FILMS_CATEGORIA = """
        SELECT * FROM v_top_films_categoria
        LIMIT 20
    """
    
    TOP_PELICULAS = """
        SELECT 
            df.film_sk,
            df.film_id,
            df.titulo,
            df.clasificacion,
            df.duracion,
            df.costo_reemplazo,
            df.tarifa_renta,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as revenue_total,
            AVG(fv.dias_renta_promedio) as dias_promedio
        FROM fact_ventas fv
        JOIN dim_film df ON fv.film_sk = df.film_sk AND df.activo = TRUE
        GROUP BY df.film_sk, df.film_id, df.titulo, df.clasificacion, 
                 df.duracion, df.costo_reemplazo, df.tarifa_renta
        ORDER BY total_rentas DESC
        LIMIT 20
    """
    
    PELICULAS_POR_CLASIFICACION = """
        SELECT 
            df.clasificacion,
            COUNT(DISTINCT df.film_id) as total_peliculas,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as revenue_total,
            AVG(df.duracion) as duracion_promedio,
            AVG(df.tarifa_renta) as tarifa_promedio
        FROM fact_ventas fv
        JOIN dim_film df ON fv.film_sk = df.film_sk AND df.activo = TRUE
        GROUP BY df.clasificacion
        ORDER BY total_rentas DESC
    """
    
    # ========== CATEGORÍAS ==========
    
    PERFORMANCE_CATEGORIA = """
        SELECT * FROM v_performance_categoria
    """
    
    VENTAS_POR_CATEGORIA = """
        SELECT 
            dc.categoria_sk,
            dc.nombre_categoria,
            COUNT(DISTINCT fv.film_sk) as total_peliculas,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as revenue_total,
            AVG(fv.monto_promedio) as ticket_promedio,
            AVG(fv.dias_renta_promedio) as dias_promedio
        FROM fact_ventas fv
        JOIN dim_categoria dc ON fv.categoria_sk = dc.categoria_sk AND dc.activo = TRUE
        GROUP BY dc.categoria_sk, dc.nombre_categoria
        ORDER BY revenue_total DESC
    """
    
    CATEGORIA_TENDENCIA_TEMPORAL = """
        SELECT 
            dt.anio,
            dt.mes,
            dt.mes_nombre,
            dc.nombre_categoria,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as revenue
        FROM fact_ventas fv
        JOIN dim_tiempo dt ON fv.fecha_id = dt.fecha_id
        JOIN dim_categoria dc ON fv.categoria_sk = dc.categoria_sk AND dc.activo = TRUE
        GROUP BY dt.anio, dt.mes, dt.mes_nombre, dc.nombre_categoria
        ORDER BY dt.anio, dt.mes, dc.nombre_categoria
    """
    
    # ========== FILTROS ==========
    
    GET_FECHAS_DISPONIBLES = """
        SELECT 
            MIN(fecha) as fecha_min,
            MAX(fecha) as fecha_max
        FROM dim_tiempo
        WHERE fecha_id IN (SELECT DISTINCT fecha_id FROM fact_ventas)
    """
    
    GET_CATEGORIAS = """
        SELECT DISTINCT categoria_sk, categoria_id, nombre_categoria
        FROM dim_categoria
        WHERE activo = TRUE
        ORDER BY nombre_categoria
    """
    
    GET_TIENDAS = """
        SELECT DISTINCT tienda_sk, tienda_id, nombre_tienda, ciudad, pais
        FROM dim_tienda
        WHERE activo = TRUE
        ORDER BY nombre_tienda
    """
    
    # ========== BÚSQUEDAS ==========
    
    BUSCAR_PELICULA = """
        SELECT 
            df.film_sk,
            df.film_id,
            df.titulo,
            df.descripcion,
            df.clasificacion,
            df.duracion,
            df.tarifa_renta,
            df.costo_reemplazo,
            COALESCE(SUM(fv.cantidad_rentas), 0) as total_rentas,
            COALESCE(SUM(fv.monto_total), 0) as revenue_total
        FROM dim_film df
        LEFT JOIN fact_ventas fv ON df.film_sk = fv.film_sk
        WHERE df.activo = TRUE 
        AND df.titulo LIKE %s
        GROUP BY df.film_sk, df.film_id, df.titulo, df.descripcion, 
                 df.clasificacion, df.duracion, df.tarifa_renta, df.costo_reemplazo
        ORDER BY total_rentas DESC
    """
    
    # ========== ANÁLISIS AVANZADO ==========
    
    CORRELACION_DURACION_RENTAS = """
        SELECT 
            df.duracion,
            df.clasificacion,
            df.tarifa_renta,
            SUM(fv.cantidad_rentas) as total_rentas,
            SUM(fv.monto_total) as revenue
        FROM fact_ventas fv
        JOIN dim_film df ON fv.film_sk = df.film_sk AND df.activo = TRUE
        GROUP BY df.duracion, df.clasificacion, df.tarifa_renta
        HAVING total_rentas > 0
        ORDER BY duracion
    """
    
    ESTACIONALIDAD = """
        SELECT 
            dt.mes,
            dt.mes_nombre,
            dt.dia_semana_nombre,
            AVG(fv.cantidad_rentas) as promedio_rentas,
            AVG(fv.monto_total) as promedio_ventas
        FROM fact_ventas fv
        JOIN dim_tiempo dt ON fv.fecha_id = dt.fecha_id
        GROUP BY dt.mes, dt.mes_nombre, dt.dia_semana_nombre
        ORDER BY dt.mes
    """
    
    # ========== DETALLE COMPLETO ==========
    
    DETALLE_VENTAS = """
        SELECT 
            dt.fecha,
            dt.anio,
            dt.mes_nombre,
            dt.dia_semana_nombre,
            df.titulo as pelicula,
            df.clasificacion,
            dc.nombre_categoria,
            ds.nombre_tienda,
            ds.ciudad,
            fv.cantidad_rentas,
            fv.monto_total,
            fv.monto_promedio,
            fv.dias_renta_promedio
        FROM fact_ventas fv
        JOIN dim_tiempo dt ON fv.fecha_id = dt.fecha_id
        JOIN dim_film df ON fv.film_sk = df.film_sk AND df.activo = TRUE
        JOIN dim_categoria dc ON fv.categoria_sk = dc.categoria_sk AND dc.activo = TRUE
        JOIN dim_tienda ds ON fv.tienda_sk = ds.tienda_sk AND ds.activo = TRUE
        ORDER BY dt.fecha DESC
        LIMIT 1000
    """