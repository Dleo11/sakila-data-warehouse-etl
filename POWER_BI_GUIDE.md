# Guía de Integración con Power BI

Instrucciones paso a paso para conectar Power BI Desktop al Data Mart Sakila y crear dashboards analíticos.

## Requisitos Previos

1. **Power BI Desktop** instalado
   - Descargar desde: https://powerbi.microsoft.com/desktop/
   - Versión recomendada: Última versión estable

2. **MySQL Connector** para Power BI
   - Descargar desde: https://dev.mysql.com/downloads/connector/net/
   - Instalar el conector .NET

3. **Data Mart poblado**
   - Haber ejecutado exitosamente `notebooks/03_transformacion.ipynb`
   - Verificar que `sakila_dw` tiene datos

---

## Paso 1: Conectar Power BI a MySQL

### 1.1 Abrir Power BI Desktop

1. Abrir Power BI Desktop
2. Click en **"Obtener datos"** (Get Data)
3. Buscar **"MySQL database"**
4. Click en **"Conectar"**

### 1.2 Configurar Conexión

Ingresar los siguientes datos:

```
Servidor: localhost
Base de datos: sakila_dw
```

Click en **"Aceptar"**

### 1.3 Credenciales

Cuando solicite credenciales:

1. Seleccionar **"Base de datos"** (Database)
2. Ingresar:
   - **Usuario**: root (o tu usuario MySQL)
   - **Contraseña**: tu password de MySQL
3. Click en **"Conectar"**

### 1.4 Navegador de Tablas

Se abrirá el navegador mostrando las tablas de `sakila_dw`:

✅ Seleccionar las siguientes tablas:
- `dim_tiempo`
- `dim_film`
- `dim_categoria`
- `dim_tienda`
- `fact_ventas`

✅ También puedes seleccionar las vistas:
- `v_top_films_categoria`
- `v_ventas_mensuales_tienda`
- `v_performance_categoria`
- `v_resumen_ejecutivo`

Click en **"Cargar"** (Load)

---

## Paso 2: Configurar Relaciones

Power BI debería detectar automáticamente las relaciones, pero verifica:

### 2.1 Ir a Vista de Modelo

1. Click en el icono **"Modelo"** (Model) en la barra izquierda
2. Verifica que existan las siguientes relaciones:

```
fact_ventas[fecha_id] → dim_tiempo[fecha_id]
fact_ventas[film_sk] → dim_film[film_sk]
fact_ventas[categoria_sk] → dim_categoria[categoria_sk]
fact_ventas[tienda_sk] → dim_tienda[tienda_sk]
```

### 2.2 Crear Relaciones Manualmente (si no existen)

Si alguna relación falta:

1. Arrastrar desde `fact_ventas.fecha_id` hacia `dim_tiempo.fecha_id`
2. En el diálogo:
   - Cardinalidad: **Muchos a uno** (*:1)
   - Dirección del filtro: **Única** (Simple)
   - Click en **"Aceptar"**

Repetir para las otras dimensiones.

---

## Paso 3: Medidas DAX Recomendadas

### 3.1 Crear Medidas Básicas

En la vista **"Datos"** (Data), click derecho en `fact_ventas` → **"Nueva medida"**

#### Total Rentas
```dax
Total Rentas = SUM(fact_ventas[cantidad_rentas])
```

#### Ingresos Totales
```dax
Ingresos Totales = SUM(fact_ventas[monto_total])
```

#### Ticket Promedio
```dax
Ticket Promedio = AVERAGE(fact_ventas[monto_promedio])
```

#### Días Renta Promedio
```dax
Días Renta Promedio = AVERAGE(fact_ventas[dias_renta_promedio])
```

### 3.2 Medidas Avanzadas

#### Rentas vs Año Anterior
```dax
Rentas YoY = 
VAR RentasActuales = [Total Rentas]
VAR RentasAñoAnterior = 
    CALCULATE(
        [Total Rentas],
        DATEADD(dim_tiempo[fecha], -1, YEAR)
    )
RETURN
    RentasActuales - RentasAñoAnterior
```

#### % Crecimiento YoY
```dax
% Crecimiento YoY = 
DIVIDE(
    [Rentas YoY],
    CALCULATE([Total Rentas], DATEADD(dim_tiempo[fecha], -1, YEAR)),
    0
) * 100
```

#### Top 10 Películas
```dax
Top 10 Películas = 
IF(
    RANKX(ALL(dim_film), [Total Rentas], , DESC) <= 10,
    [Total Rentas],
    BLANK()
)
```

#### Concentración de Ventas (% del Total)
```dax
% del Total = 
DIVIDE(
    [Ingresos Totales],
    CALCULATE([Ingresos Totales], ALL(dim_categoria)),
    0
) * 100
```

---

## Paso 4: Crear Visualizaciones

### Dashboard 1: Resumen Ejecutivo

**KPIs Principales (Tarjetas):**
- Total Rentas
- Ingresos Totales
- Ticket Promedio
- Días Renta Promedio

**Gráfico de Líneas:** Evolución Mensual
- Eje X: `dim_tiempo[mes_nombre]` o `dim_tiempo[fecha]`
- Eje Y: `Ingresos Totales`
- Leyenda: `dim_tienda[nombre_tienda]`

**Gráfico de Barras:** Top 10 Categorías
- Eje Y: `dim_categoria[nombre_categoria]`
- Eje X: `Ingresos Totales`
- Ordenar por: Ingresos Totales (DESC)

**Gráfico Circular:** Distribución por Tienda
- Valores: `Ingresos Totales`
- Leyenda: `dim_tienda[ciudad]`

### Dashboard 2: Análisis de Películas

**Tabla:** Top 20 Películas
- Columnas:
  - `dim_film[titulo]`
  - `dim_categoria[nombre_categoria]`
  - `Total Rentas`
  - `Ingresos Totales`
  - `Ticket Promedio`

**Matriz:** Películas por Categoría
- Filas: `dim_categoria[nombre_categoria]`
- Columnas: `dim_tiempo[anio]`
- Valores: `Total Rentas`

**Gráfico de Dispersión:** Precio vs Popularidad
- Eje X: `dim_film[tarifa_renta]`
- Eje Y: `Total Rentas`
- Detalles: `dim_film[titulo]`
- Tamaño: `Ingresos Totales`

### Dashboard 3: Performance por Tienda

**Mapa:** Ventas por Ubicación
- Ubicación: `dim_tienda[ciudad]`, `dim_tienda[pais]`
- Tamaño: `Ingresos Totales`
- Color: `Total Rentas`

**Gráfico de Columnas Agrupadas:** Comparación Mensual
- Eje X: `dim_tiempo[mes_nombre]`
- Eje Y: `Ingresos Totales`
- Leyenda: `dim_tienda[nombre_tienda]`

**Tabla de Comparación:**
- Filas: `dim_tienda[nombre_tienda]`
- Valores:
  - `Total Rentas`
  - `Ingresos Totales`
  - `Ticket Promedio`
  - `% del Total`

---

## Paso 5: Segmentadores (Slicers)

Agregar segmentadores para filtrado interactivo:

**Fecha:**
- Tipo: Rango de fechas
- Campo: `dim_tiempo[fecha]`

**Año:**
- Tipo: Lista
- Campo: `dim_tiempo[anio]`

**Categoría:**
- Tipo: Lista desplegable
- Campo: `dim_categoria[nombre_categoria]`

**Tienda:**
- Tipo: Botones
- Campo: `dim_tienda[nombre_tienda]`

---

## Paso 6: Configuración de Actualización

### 6.1 Actualización Manual

1. Click en **"Inicio"** → **"Actualizar"**
2. Power BI ejecutará las queries nuevamente

### 6.2 Actualización Programada (Power BI Service)

Para publicar en Power BI Service con actualización automática:

1. **Publicar el reporte:**
   - Click en **"Inicio"** → **"Publicar"**
   - Seleccionar workspace

2. **Configurar Gateway:**
   - Instalar Power BI Gateway en servidor con acceso a MySQL
   - Configurar credenciales

3. **Programar actualización:**
   - En Power BI Service, ir al dataset
   - Configurar actualización programada (diaria/semanal)

---

## Queries Directas vs Importación

### Importación (Recomendado)

**Ventajas:**
- Rendimiento rápido
- Funciona sin conexión
- Soporta todas las funciones DAX

**Desventajas:**
- Necesita actualización manual/programada
- Límite de tamaño (1GB en versión gratuita)

### DirectQuery

**Ventajas:**
- Datos siempre actualizados
- No hay límite de tamaño

**Desventajas:**
- Rendimiento depende de MySQL
- No todas las funciones DAX disponibles

**Recomendación:** Usar **Importación** para este proyecto (volumen de datos moderado).

---

## Optimizaciones

### Reducir Tamaño del Modelo

Si el modelo es muy grande:

1. **Eliminar columnas innecesarias:**
   - En vista **"Datos"**, ocultar columnas no utilizadas
   - Click derecho → **"Ocultar"**

2. **Usar columnas calculadas solo cuando necesario:**
   - Preferir medidas DAX sobre columnas calculadas

3. **Filtrar datos en el origen:**
   - En Power Query, filtrar por año
   - Ejemplo: Solo últimos 2 años

### Mejorar Rendimiento de Visualizaciones

1. **Limitar filas en tablas:**
   - Usar Top N en filtros visuales
   - Máximo 20-50 filas por tabla

2. **Agregaciones previas:**
   - Usar las vistas (`v_*`) en lugar de fact_ventas para reportes simples

3. **Índices en MySQL:**
   - Ya implementados en `create_datamart.sql`

---

## Ejemplo de Dashboard Completo

### Layout Sugerido

```
┌─────────────────────────────────────────────────────────────┐
│  SAKILA DATA WAREHOUSE - DASHBOARD EJECUTIVO                │
│                                                              │
│  [Filtros: Año] [Categoría] [Tienda]                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  16,044  │  │ $67,416  │  │  $4.20   │  │  5.2     │  │
│  │  Rentas  │  │ Ingresos │  │  Ticket  │  │  Días    │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│                                                              │
│  ┌─────────────────────────────┐  ┌───────────────────┐   │
│  │ Evolución Mensual           │  │ Top Categorías    │   │
│  │ (Gráfico de Líneas)         │  │ (Barras)          │   │
│  │                             │  │                   │   │
│  └─────────────────────────────┘  └───────────────────┘   │
│                                                              │
│  ┌─────────────────────────────┐  ┌───────────────────┐   │
│  │ Top 10 Películas            │  │ Por Tienda        │   │
│  │ (Tabla)                     │  │ (Gráfico Circular)│   │
│  │                             │  │                   │   │
│  └─────────────────────────────┘  └───────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### Error: "No se puede conectar al servidor"

**Solución:**
1. Verificar que MySQL esté corriendo
2. Verificar que el puerto 3306 esté abierto
3. Verificar firewall de Windows

### Error: "Credenciales incorrectas"

**Solución:**
1. Verificar usuario y contraseña en MySQL Workbench
2. En Power BI: Archivo → Opciones → Orígenes de datos → Limpiar permisos

### Error: "No se pueden cargar las tablas"

**Solución:**
1. Verificar que `sakila_dw` tenga datos
2. Ejecutar `notebooks/03_transformacion.ipynb`
3. Verificar permisos del usuario MySQL

### Rendimiento Lento

**Solución:**
1. Reducir período de datos (último año solamente)
2. Usar vistas en lugar de fact_ventas
3. Agregar más RAM a la computadora

---

## Recursos Adicionales

**Documentación Oficial:**
- Power BI Desktop: https://powerbi.microsoft.com/documentation/
- DAX Reference: https://dax.guide/
- MySQL Connector: https://dev.mysql.com/doc/connector-net/en/

**Tutoriales:**
- Power BI con MySQL: https://www.youtube.com/results?search_query=power+bi+mysql
- Modelado de Datos: https://docs.microsoft.com/power-bi/guidance/star-schema

---

## Checklist de Entrega

- [ ] Conexión exitosa a `sakila_dw`
- [ ] Relaciones configuradas correctamente
- [ ] Al menos 3 dashboards creados
- [ ] 5+ medidas DAX implementadas
- [ ] Segmentadores funcionales
- [ ] Visualizaciones con datos reales
- [ ] Archivo .pbix guardado
- [ ] Screenshots de dashboards para documentación

---

¡Dashboard listo para análisis de negocio!
</parameter>
</invoke>