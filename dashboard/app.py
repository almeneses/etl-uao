import datetime

import pandas as pd
import streamlit as st
from utils.db_utils import (obtener_contaminantes_por_estacion,
                            obtener_estaciones, obtener_indice_ica,
                            obtener_mediciones)
from utils.ica_utils import obtener_color_por_categoria
from utils.logging_utils import get_logger
from utils.kpi_utils import calcular_kpis_estacion
from utils.normativa_utils import obtener_limites
from utils.plot_utils import (COLOR_POR_CONTAMINANTE, agregar_frecuencia,
                              color_fila, hex_to_rgba,
                              plot_heatmap_interactivo_horario,
                              plot_heatmaps_por_contaminante,
                              plot_linea_comparativa, plot_matriz_correlacion)

logger = get_logger()


# Carga de datos con caché
@st.cache_data(ttl=600)
def load_data(id_est, id_cont, fecha_ini=None, fecha_fin=None):
    """
    Carga las mediciones de la base de datos para una estación y un contaminante.

    Los resultados se cachean por 10 minutos para mejorar el desempeño del
    dashboard.

    Parámetros:
        id_est (int): ID de la estación a consultar.
        id_cont (int): ID del contaminante a consultar.
        fecha_ini (str | None): Fecha inicial (YYYY-MM-DD) o None para sin filtro.
        fecha_fin (str | None): Fecha final (YYYY-MM-DD) o None para sin filtro.

    Retorna:
        pd.DataFrame: DataFrame con las columnas de medición, incluyendo
        la columna 'fecha_hora' convertida a tipo datetime si hay datos.
    """
    logger.info(
        "Cargando mediciones: estacion=%s contaminante=%s fecha_ini=%s fecha_fin=%s",
        id_est,
        id_cont,
        fecha_ini,
        fecha_fin,
    )

    df = obtener_mediciones(id_est, id_cont, fecha_ini, fecha_fin)
    if not df.empty:
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
    return df


# Filtros principales (dinámicos por estación)
def draw_filters():
    """
    Dibuja los filtros principales en la barra lateral del dashboard.

    Permite seleccionar la estación, los contaminantes disponibles para esa
    estación, un rango de fechas y la frecuencia temporal de agregación.

    Retorna:
        tuple: (
            estacion,
            id_est,
            contaminantes_sel,
            rango_fechas,
            frecuencia,
            estaciones_df,
            contaminantes_df,
        ) donde:
            - estacion (str): Nombre de la estación seleccionada.
            - id_est (int): ID de la estación seleccionada.
            - contaminantes_sel (list[str]): Contaminantes elegidos por el usuario.
            - rango_fechas (list[datetime.date] | tuple[datetime.date, datetime.date]):
              Fechas seleccionadas en el control de rango.
            - frecuencia (str): "Hora", "Día" o "Mes".
            - estaciones_df (pd.DataFrame): Catálogo de estaciones disponibles.
            - contaminantes_df (pd.DataFrame): Contaminantes de la estación seleccionada.
    """
    st.sidebar.header("🎛️ Filtros principales")

    estaciones_df = obtener_estaciones()
    estacion = st.sidebar.selectbox("Estación", estaciones_df["nombre"])

    # Obtener ID y contaminantes asociados a la estación seleccionada
    id_est = int(estaciones_df.loc[estaciones_df["nombre"] == estacion, "id_estacion"].values[0])
    contaminantes_df = obtener_contaminantes_por_estacion(id_est)

    # Si la estación no tiene contaminantes, mostramos aviso
    if contaminantes_df.empty:
        st.sidebar.warning("Esta estación no tiene contaminantes registrados.")
        contaminantes_sel = []
    else:
        contaminantes_sel = st.sidebar.multiselect(
            "Contaminantes medidos",
            contaminantes_df["nombre"].tolist(),
            default=[contaminantes_df["nombre"].iloc[0]]
        )

    # Limitar rango de fechas: desde 2000 hasta hoy
    fecha_min = datetime.date(2000, 1, 1)
    fecha_max = datetime.date.today()
    rango_fechas = st.sidebar.date_input(
        "Rango de fechas",
        value=[],
        min_value=fecha_min,
        max_value=fecha_max,
        help=f"Selecciona fechas entre {fecha_min.year} y {fecha_max.strftime('%Y-%m-%d')}"
    )

    frecuencia = st.sidebar.radio(
        "Frecuencia temporal",
        options=["Hora", "Día", "Mes"],
        horizontal=True
    )

    return estacion, id_est, contaminantes_sel, rango_fechas, frecuencia, estaciones_df, contaminantes_df


# Controles normativos
def draw_norma_controls():
    """
    Muestra en la barra lateral los controles de visualización normativa.

    Incluye opciones para mostrar referencias de la OMS e IDEAM y la
    opción para sombrear las zonas de excedencia en los gráficos.

    Retorna:
        tuple: (mostrar_oms, mostrar_ideam, sombrear_zona)
            - mostrar_oms (bool): Si se muestran los límites de la OMS.
            - mostrar_ideam (bool): Si se muestran los límites del IDEAM.
            - sombrear_zona (bool): Si se sombrea el área por encima del límite.
    """
    st.sidebar.header("📏 Controles normativos")
    mostrar_oms = st.sidebar.checkbox("Mostrar OMS", value=True)
    mostrar_ideam = st.sidebar.checkbox("Mostrar IDEAM", value=True)
    sombrear_zona = st.sidebar.checkbox("Sombrear excedencias", value=True)

    return mostrar_oms, mostrar_ideam, sombrear_zona



# Gráfica comparativa
def show_comparison_chart(id_est, contaminantes_sel, frecuencia,
                          fecha_ini, fecha_fin,
                          mostrar_oms, mostrar_ideam, sombrear_zona):
    """
    Genera y renderiza una gráfica comparativa de los contaminantes seleccionados.

    Parámetros:
        id_est (int): ID de la estación.
        contaminantes_sel (list[str]): Nombres de los contaminantes a comparar.
        frecuencia (str): Frecuencia temporal ("Hora", "Día" o "Mes").
        fecha_ini (str | None): Fecha inicial (YYYY-MM-DD) o None para sin filtro.
        fecha_fin (str | None): Fecha final (YYYY-MM-DD) o None para sin filtro.
        mostrar_oms (bool): Si se muestran las referencias de la OMS.
        mostrar_ideam (bool): Si se muestran las referencias del IDEAM.
        sombrear_zona (bool): Si se sombrea el área por encima del límite.
    """
    st.subheader("📈 Comparación entre contaminantes")
    fig = plot_linea_comparativa(
        id_est=id_est,
        contaminantes=contaminantes_sel,
        frecuencia=frecuencia,
        fecha_ini=fecha_ini,
        fecha_fin=fecha_fin,
        mostrar_oms=mostrar_oms,
        mostrar_ideam=mostrar_ideam,
        sombrear=sombrear_zona
    )
    st.plotly_chart(fig, use_container_width=True)


def show_heatmaps(id_est, contaminantes_sel, contaminantes_df, fecha_ini, fecha_fin):
    """
    Muestra mapas de calor de las mediciones según los contaminantes seleccionados.

    - Si hay un único contaminante, se renderiza un mapa de calor horario
      interactivo con los datos de dicha serie.
    - Si hay múltiples contaminantes, se generan heatmaps individuales por cada uno.

    Parámetros:
        id_est (int): ID de la estación.
        contaminantes_sel (list[str]): Nombres de contaminantes seleccionados.
        contaminantes_df (pd.DataFrame): Catálogo de contaminantes para la estación.
        fecha_ini (str | None): Fecha inicial (YYYY-MM-DD) o None.
        fecha_fin (str | None): Fecha final (YYYY-MM-DD) o None.
    """
    if len(contaminantes_sel) == 1:
        cont = contaminantes_sel[0]
        id_cont = int(contaminantes_df.loc[contaminantes_df["nombre"] == cont, "id_contaminante"].values[0])
        df = load_data(id_est, id_cont, fecha_ini, fecha_fin)
        if not df.empty:
            st.subheader("🌡️ Mapa de calor horario")
            fig_heatmap = plot_heatmap_interactivo_horario(df, cont)
            st.plotly_chart(fig_heatmap, use_container_width=True)
    elif len(contaminantes_sel) > 1:
        plot_heatmaps_por_contaminante(id_est, contaminantes_sel, fecha_ini, fecha_fin)


# Tarjeta resumen ICA
def show_ica_summary(id_est, fecha_ini=None, fecha_fin=None):
    """
    Muestra una tarjeta resumen del Índice de Calidad del Aire (ICA).

    Obtiene el ICA para la estación y rango de fechas indicados (si la
    tabla/funcionalidad está disponible). Renderiza una tarjeta con la
    categoría predominante y el valor promedio del índice.

    Parámetros:
        id_est (int): ID de la estación.
        fecha_ini (str | None): Fecha inicial (YYYY-MM-DD) o None.
        fecha_fin (str | None): Fecha final (YYYY-MM-DD) o None.
    """
    try:
        df_ica = obtener_indice_ica(id_est, fecha_ini, fecha_fin)
    except Exception:
        logger.exception(
            "Error obteniendo índice ICA para estacion=%s rango=%s-%s",
            id_est,
            fecha_ini,
            fecha_fin,
        )
        st.info("El índice ICA aún no está disponible en esta base de datos.")
        return

    if df_ica.empty:
        st.warning("No hay datos de índice ICA para esta estación o rango.")
        return

    ica_promedio = df_ica["ica"].mean()
    categoria_mas_frecuente = df_ica["categoria"].mode()[0]
    color = obtener_color_por_categoria(categoria_mas_frecuente)

    st.markdown("### 🌫️ Estado de la calidad del aire")
    st.markdown(
        f"""
        <div style="background-color:{color}; padding:1.2em; border-radius:12px; color:white; text-align:center;">
            <h3 style="margin:0;">{categoria_mas_frecuente}</h3>
            <h2 style="margin:0;">ICA promedio: {ica_promedio:.1f}</h2>
        </div>
        """,
        unsafe_allow_html=True
    )

def show_kpi_table(id_est, contaminantes_sel, fecha_ini, fecha_fin):
    st.subheader("📊 Indicadores clave por contaminante")

    df_kpi = calcular_kpis_estacion(id_est, contaminantes_sel, fecha_ini, fecha_fin)
    if df_kpi.empty:
        st.info("No hay datos suficientes para calcular indicadores en el rango seleccionado.")
        return

    # --- Asegurar tipos numéricos ---
    for col in ["Promedio (µg/m³)", "Máximo (µg/m³)", "% Norma IDEAM (24h)"]:
        df_kpi[col] = pd.to_numeric(df_kpi[col], errors="coerce")
        
    # --- Estilo base ---
    df_kpi_styled = (
        df_kpi.style
        .format({
            "Promedio (µg/m³)": lambda x: f"{x:.2f}" if pd.notna(x) else "N/A",
            "Máximo (µg/m³)": lambda x: f"{x:.2f}" if pd.notna(x) else "N/A",
            "% Norma IDEAM (24h)": lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
        })
        .apply(color_fila, axis=1)
        .set_properties(**{
            "text-align": "center",
            "font-weight": "500",
            "border-color": "#e0e0e0",
            "border-width": "1px",
            "border-style": "solid"
        })
    )

    st.dataframe(df_kpi_styled, use_container_width=True)

def main():
    """
    Punto de entrada principal del dashboard de calidad del aire.

    Configura la página, muestra los filtros, controles normativos y
    renderiza: gráfica comparativa, tarjeta de ICA, heatmaps y matriz de
    correlación (cuando aplique) para los contaminantes seleccionados.
    """
    st.set_page_config(page_title="Dashboard Calidad del Aire", layout="wide")
    st.title("🌍 Dashboard de Calidad del Aire – Comparativo y Normativo")

    estacion, id_est, contaminantes_sel, rango_fechas, frecuencia, estaciones_df, contaminantes_df = draw_filters()
    mostrar_oms, mostrar_ideam, sombrear_zona = draw_norma_controls()

    if not contaminantes_sel:
        st.info("Selecciona al menos un contaminante para continuar.")
        return

    fecha_ini = str(rango_fechas[0]) if len(rango_fechas) > 0 else None
    fecha_fin = str(rango_fechas[1]) if len(rango_fechas) > 1 else None

    logger.info(
        "Filtros seleccionados: estacion=%s contaminantes=%s fecha_ini=%s fecha_fin=%s frecuencia=%s",
        estacion,
        ", ".join(contaminantes_sel),
        fecha_ini,
        fecha_fin,
        frecuencia,
    )

    # Tarjeta ICA
    show_ica_summary(id_est, fecha_ini, fecha_fin)
    st.divider()

    # Grafico lineas
    show_comparison_chart(id_est, contaminantes_sel, frecuencia,
                          fecha_ini, fecha_fin,
                          mostrar_oms, mostrar_ideam, sombrear_zona)
    st.divider()
    # Tabla KPI
    show_kpi_table(id_est, contaminantes_sel, fecha_ini, fecha_fin)
    
    st.divider()
    
    # Heatmaps
    show_heatmaps(id_est, contaminantes_sel, contaminantes_df, fecha_ini, fecha_fin)

    # Matriz de correlación
    if len(contaminantes_sel) >= 2:
        st.divider()
        st.subheader("📊 Matriz de correlación entre contaminantes")
        fig_corr = plot_matriz_correlacion(id_est, contaminantes_sel, fecha_ini, fecha_fin)
        if fig_corr:
            st.plotly_chart(fig_corr, use_container_width=True)
        else:
            st.info("No hay suficientes datos comunes para calcular la correlación.")

if __name__ == "__main__":
    main()
