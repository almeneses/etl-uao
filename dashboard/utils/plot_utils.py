
from typing import Dict, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from utils.db_utils import obtener_contaminantes, obtener_mediciones
from utils.normativa_utils import obtener_limites

COLOR_POR_CONTAMINANTE = {
    "PM2,5": "#e74c3c",      # rojo fuerte
    "PM10": "#d35400",       # naranja
    "O3": "#3498db",         # azul
    "SO2": "#2ecc71",        # verde
    "NO2": "#9b59b6",        # violeta
    "CO": "#7f8c8d",         # gris
    "H2S": "#e84393",        # rosa
    "Humedad": "#1abc9c",    # turquesa
    "Temperatura": "#f39c12" # ámbar
}

def _anotar_linea_norma(fig: go.Figure, y: float, etiqueta: str, color: str):
    fig.add_hline(
        y=y,
        line_dash="dot",
        line_color=color,
        annotation_text=etiqueta,
        annotation_position="top left",
        annotation_font=dict(size=12, color=color),
        opacity=0.9
    )

def _sombrear_sobre_limite(fig: go.Figure, y: float, color: str):
    fig.add_shape(
        type="rect",
        xref="paper", yref="y",
        x0=0, x1=1, y0=y, y1=1.0,
        fillcolor=color, opacity=0.08, line_width=0, layer="below"
    )


def agregar_frecuencia(df: pd.DataFrame, frecuencia: str) -> pd.DataFrame:
    """
    Agrupa las mediciones según la frecuencia seleccionada:
    - 'Hora': deja los datos tal cual.
    - 'Día': promedia por fecha.
    - 'Mes': promedia por mes.
    """
    df = df.copy()
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])

    if frecuencia == "Hora":
        df_agg = df
    elif frecuencia == "Día":
        df_agg = (
            df.groupby(df["fecha_hora"].dt.date)["valor"]
            .mean()
            .reset_index()
            .rename(columns={"fecha_hora": "fecha"})
        )
        df_agg["fecha_hora"] = pd.to_datetime(df_agg["fecha"])
    elif frecuencia == "Mes":
        df_agg = (
            df.groupby(df["fecha_hora"].dt.to_period("M"))["valor"]
            .mean()
            .reset_index()
        )
        df_agg["fecha_hora"] = df_agg["fecha_hora"].dt.to_timestamp()
    else:
        df_agg = df

    return df_agg

def plot_linea_interactiva(df, estacion, contaminante, frecuencia,
                           limites=None, mostrar_oms=True,
                           mostrar_ideam=True, sombrear=True):
    """Gráfico de línea interactivo con eje X y hover adaptados a la frecuencia."""
    color = COLOR_POR_CONTAMINANTE.get(contaminante, "#636EFA")

    if frecuencia == "Hora":
        date_format = "%Y-%m-%d %H:%M"
        tickformat = "%b %d<br>%H:%M"
        hoverformat = "%Y-%m-%d %H:%M"
    elif frecuencia == "Día":
        date_format = "%Y-%m-%d"
        tickformat = "%b %d"
        hoverformat = "%Y-%m-%d"
    elif frecuencia == "Mes":
        date_format = "%Y-%m"
        tickformat = "%b<br>%Y"
        hoverformat = "%b %Y"
    else:
        date_format = "%Y-%m-%d %H:%M"
        tickformat = "%b %d<br>%H:%M"
        hoverformat = "%Y-%m-%d %H:%M"

    # Formatear fecha
    df = df.copy()
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
    df["fecha_formateada"] = df["fecha_hora"].dt.strftime(date_format)

    fig = px.line(
        df,
        x="fecha_hora",
        y="valor",
        title=f"Concentración promedio ({frecuencia}) - {contaminante} ({estacion})",
        labels={"fecha_hora": "Fecha", "valor": "Concentración (µg/m³)"},
        template="plotly_white",
        color_discrete_sequence=[color]
    )

    # Hover
    fig.update_traces(
        line=dict(width=5),
        hovertemplate=(
            f"<b>{contaminante}</b><br>"
            "%{x|"+hoverformat+"}<br>"
            "%{y:.2f} µg/m³<extra></extra>"
        )
    )

    # Ajuste de eje X según frecuencia
    fig.update_xaxes(
        tickformat=tickformat,
        title_text="Fecha",
        showgrid=True
    )

    # Layout general
    fig.update_layout(
        hovermode="x unified",
        title_x=0.5,
        margin=dict(l=30, r=30, t=60, b=30),
        yaxis_title="Concentración (µg/m³)"
    )

    #  Líneas de referencia (OMS/IDEAM)
    if limites:
        prioridad = ["24h", "8h", "anual"]
        if mostrar_oms and "OMS" in limites:
            for t in prioridad:
                if t in limites["OMS"]:
                    y = float(limites["OMS"][t])
                    fig.add_hline(
                        y=y,
                        line_dash="dot",
                        line_color=color,
                        annotation_text=f"OMS {t}: {y}",
                        annotation_font=dict(size=11, color=color)
                    )
                    if sombrear:
                        fig.add_shape(
                            type="rect",
                            xref="paper", yref="y",
                            x0=0, x1=1, y0=y, y1=1.0,
                            fillcolor=color, opacity=0.05,
                            line_width=0, layer="below"
                        )
                    break
        if mostrar_ideam and "IDEAM" in limites:
            for t in prioridad:
                if t in limites["IDEAM"]:
                    y = float(limites["IDEAM"][t])
                    fig.add_hline(
                        y=y,
                        line_dash="dot",
                        line_color=color,
                        annotation_text=f"IDEAM {t}: {y}",
                        annotation_font=dict(size=11, color=color)
                    )
                    if sombrear:
                        fig.add_shape(
                            type="rect",
                            xref="paper", yref="y",
                            x0=0, x1=1, y0=y, y1=1.0,
                            fillcolor=color, opacity=0.04,
                            line_width=0, layer="below"
                        )
                    break

    return fig

#Heatmap SOLO horario
def plot_heatmap_interactivo_horario(df, contaminante):
    """Heatmap horario (hora vs día) con hover personalizado."""
    df = df.copy()
    
    color = COLOR_POR_CONTAMINANTE.get(contaminante, "#636EFA")

    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
    df["fecha"] = df["fecha_hora"].dt.date
    df["hora"] = df["fecha_hora"].dt.hour

    pivot = df.pivot_table(index="hora", columns="fecha", values="valor", aggfunc="mean")

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            colorscale=[
                    [0, "white"],
                    [1, color]
                ],
            colorbar=dict(title="Concentración (µg/m³)"),
            hovertemplate=(
                "📅 <b>Fecha:</b> %{x}<br>"
                "🕐 <b>Hora:</b> %{y}:00<br>"
                "🌫️ <b>Concentración:</b> %{z:.2f} µg/m³<extra></extra>"
            )
        )
    )

    fig.update_layout(
        title=f"Mapa de calor horario (hora vs día) – {contaminante}",
        xaxis_title="Fecha",
        yaxis_title="Hora del día",
        template="plotly_white",
        height=500,
        margin=dict(l=40, r=40, t=60, b=40)
    )
    return fig


def plot_heatmaps_por_contaminante(id_est, contaminantes, fecha_ini=None, fecha_fin=None):
    """
    Muestra una grilla de heatmaps por contaminante seleccionado.
    """
    n = len(contaminantes)
    if n == 0:
        st.info("Selecciona al menos un contaminante para mostrar los heatmaps.")
        return

    cols = 2 if n <= 4 else 3
    st.markdown("### 🌡️ Mapas de calor por contaminante")
    grid_cols = st.columns(cols)

    df_contaminantes = obtener_contaminantes()

    for idx, cont in enumerate(contaminantes):
        try:
            id_cont = int(df_contaminantes.loc[df_contaminantes["nombre"] == cont, "id_contaminante"].values[0])
        except IndexError:
            grid_cols[idx % cols].warning(f"No se encontró ID para {cont}.")
            continue

        df = obtener_mediciones(id_est, id_cont, fecha_ini, fecha_fin)
        if df.empty:
            grid_cols[idx % cols].warning(f"Sin datos para {cont}.")
            continue

        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
        df["fecha"] = df["fecha_hora"].dt.date
        df["hora"] = df["fecha_hora"].dt.hour

        pivot = df.pivot_table(index="hora", columns="fecha", values="valor", aggfunc="mean")
        color = COLOR_POR_CONTAMINANTE.get(cont, "#636EFA")

        # Heatmap con hover personalizado
        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale=[[0, "white"], [1, color]],
                colorbar=dict(title="Concentración (µg/m³)"),
                hovertemplate=(
                    "📅 <b>Fecha:</b> %{x}<br>"
                    "🕐 <b>Hora:</b> %{y}:00<br>"
                    "🌫️ <b>Concentración:</b> %{z:.2f} µg/m³<extra></extra>"
                )
            )
        )

        fig.update_layout(
            title=f"{cont}",
            xaxis_title="Fecha",
            yaxis_title="Hora del día",
            template="plotly_white",
            height=320,
            margin=dict(l=10, r=10, t=40, b=30)
        )

        grid_cols[idx % cols].plotly_chart(fig, use_container_width=True)




def plot_linea_comparativa(id_est, contaminantes, frecuencia, fecha_ini=None, fecha_fin=None,
                           mostrar_oms=True, mostrar_ideam=True, sombrear=True):
    """
    Grafica múltiples contaminantes en una misma estación con colores diferenciados.
    """
    fig = go.Figure()
    prioridad = ["24h", "8h", "anual"]

    # Obtener DataFrame base de contaminantes
    df_contaminantes = obtener_contaminantes()

    for cont in contaminantes:
        try:
            id_cont = int(df_contaminantes.loc[df_contaminantes["nombre"] == cont, "id_contaminante"].values[0])
        except IndexError:
            continue

        # Obtener mediciones y agregar por frecuencia
        df = obtener_mediciones(id_est, id_cont, fecha_ini, fecha_fin)
        if df.empty:
            continue
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
        df_agg = agregar_frecuencia(df, frecuencia)

        # Obtener color definido (o uno por defecto)
        color = COLOR_POR_CONTAMINANTE.get(cont, None)
        if color is None:
            color = f"hsl({hash(cont) % 360},70%,50%)"  # color determinista si no está definido

        # Línea del contaminante
        fig.add_trace(go.Scatter(
            x=df_agg["fecha_hora"],
            y=df_agg["valor"],
            mode="lines",
            name=cont,
            line=dict(color=color, width=1),  # ✅ color fijo
            hovertemplate=f"{cont}<br>%{{x}}<br>%{{y:.2f}} µg/m³<extra></extra>"
        ))

        # Límites normativos
        limites = obtener_limites(cont)
        if limites:
            if mostrar_oms and "OMS" in limites:
                for t in prioridad:
                    if t in limites["OMS"]:
                        y = float(limites["OMS"][t])
                        fig.add_hline(
                            y=y, line_dash="dot", line_color=color,
                            annotation_text=f"{cont} OMS {t}: {y}",
                            annotation_font=dict(size=10, color=color)
                        )
                        if sombrear:
                            fig.add_shape(
                                type="rect",
                                xref="paper", yref="y",
                                x0=0, x1=1, y0=y, y1=1.0,
                                fillcolor=color, opacity=0.05, line_width=0, layer="below"
                            )
                        break
            if mostrar_ideam and "IDEAM" in limites:
                for t in prioridad:
                    if t in limites["IDEAM"]:
                        y = float(limites["IDEAM"][t])
                        fig.add_hline(
                            y=y, line_dash="dot", line_color=color,
                            annotation_text=f"{cont} IDEAM {t}: {y}",
                            annotation_font=dict(size=10, color=color)
                        )
                        if sombrear:
                            fig.add_shape(
                                type="rect",
                                xref="paper", yref="y",
                                x0=0, x1=1, y0=y, y1=1.0,
                                fillcolor=color, opacity=0.04, line_width=0, layer="below"
                            )
                        break

    fig.update_layout(
        title=f"Comparación de contaminantes – Estación {id_est}",
        xaxis_title="Fecha",
        yaxis_title="Concentración (µg/m³)",
        hovermode="x unified",
        template="plotly_white",
        legend_title="Contaminantes",
        margin=dict(l=30, r=30, t=60, b=30)
    )

    return fig


def plot_heatmaps_por_contaminante(id_est, contaminantes, fecha_ini=None, fecha_fin=None):
    """
    Muestra una grilla de heatmaps por contaminante seleccionado.
    """
    n = len(contaminantes)
    if n == 0:
        st.info("Selecciona al menos un contaminante para mostrar los heatmaps.")
        return

    cols = 2 if n <= 4 else 3
    rows = (n + cols - 1) // cols

    st.markdown("### 🌡️ Mapas de calor por contaminante")
    grid_cols = st.columns(cols)

    from utils.db_utils import obtener_contaminantes
    df_contaminantes = obtener_contaminantes()

    for idx, cont in enumerate(contaminantes):
        id_cont = int(df_contaminantes.loc[df_contaminantes["nombre"] == cont, "id_contaminante"].values[0])
        df = obtener_mediciones(id_est, id_cont, fecha_ini, fecha_fin)

        if df.empty:
            grid_cols[idx % cols].warning(f"Sin datos para {cont}.")
            continue

        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
        df["fecha"] = df["fecha_hora"].dt.date
        df["hora"] = df["fecha_hora"].dt.hour

        pivot = df.pivot_table(index="hora", columns="fecha", values="valor", aggfunc="mean")

        color = COLOR_POR_CONTAMINANTE.get(cont, "#636EFA")

        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale=[
                    [0, "white"],
                    [1, color]
                ],
                colorbar=dict(title="Concentración")
            )
        )
        fig.update_layout(
            title=f"{cont}",
            xaxis_title="Fecha",
            yaxis_title="Hora del día",
            template="plotly_white",
            height=300,
            margin=dict(l=10, r=10, t=40, b=30)
        )
        grid_cols[idx % cols].plotly_chart(fig, use_container_width=True)


def plot_matriz_correlacion(id_est, contaminantes, fecha_ini=None, fecha_fin=None):
    """
    Calcula y muestra una matriz de correlación entre contaminantes seleccionados.
    """
    from utils.db_utils import obtener_contaminantes, obtener_mediciones

    df_contaminantes = obtener_contaminantes()
    data_comb = {}

    # Recoger datos de cada contaminante
    for cont in contaminantes:
        try:
            id_cont = int(df_contaminantes.loc[df_contaminantes["nombre"] == cont, "id_contaminante"].values[0])
        except IndexError:
            continue

        df = obtener_mediciones(id_est, id_cont, fecha_ini, fecha_fin)
        if df.empty:
            continue

        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
        df = df.groupby("fecha_hora")["valor"].mean().reset_index()
        data_comb[cont] = df.set_index("fecha_hora")["valor"]

    if not data_comb:
        return None

    # Unir los contaminantes por fecha
    df_all = pd.concat(data_comb, axis=1).dropna()

    if df_all.empty or len(df_all.columns) < 2:
        return None

    # Calcular matriz de correlación
    corr = df_all.corr(method="pearson").round(2)

    # Crear heatmap interactivo
    fig = px.imshow(
        corr,
        text_auto=True,
        color_continuous_scale="RdBu_r",
        title=None,
        labels=dict(x="Contaminante", y="Contaminante", color="Correlación"),
        zmin=-1,
        zmax=1
    )
    fig.update_layout(
        width=700,
        height=700,
        margin=dict(l=60, r=60, t=80, b=60),
        title_x=0.5,
        template="plotly_white"
    )
    return fig
