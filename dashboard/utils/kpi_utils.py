import pandas as pd
import numpy as np
from utils.db_utils import obtener_mediciones, obtener_contaminantes
from utils.normativa_utils import obtener_limites

def calcular_kpis_estacion(id_est, contaminantes, fecha_ini=None, fecha_fin=None):
    """
    Calcula indicadores clave (KPI) por contaminante para la estación seleccionada.
    """
    resultados = []
    df_contaminantes = obtener_contaminantes()

    for cont in contaminantes:
        try:
            id_cont = int(df_contaminantes.loc[df_contaminantes["nombre"] == cont, "id_contaminante"].values[0])
        except IndexError:
            continue

        df = obtener_mediciones(id_est, id_cont, fecha_ini, fecha_fin)
        if df.empty:
            continue

        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
        df = df.sort_values("fecha_hora")

        promedio = df["valor"].mean()
        maximo = df["valor"].max()

        # Comparar con IDEAM
        limites = obtener_limites(cont)
        limite_ideam = None
        if "IDEAM" in limites and "24h" in limites["IDEAM"]:
            limite_ideam = float(limites["IDEAM"]["24h"])
        porcentaje_norma = (promedio / limite_ideam * 100) if limite_ideam else np.nan

        # Tendencia
        n = len(df)
        if n >= 8:
            inicio = df.head(int(n * 0.25))["valor"].mean()
            fin = df.tail(int(n * 0.25))["valor"].mean()
            tendencia = "↑" if fin > inicio else "↓" if fin < inicio else "→"
        else:
            tendencia = "—"

        resultados.append({
            "Contaminante": cont,
            "Promedio (µg/m³)": round(promedio, 2),
            "Máximo (µg/m³)": round(maximo, 2),
            "% Norma IDEAM (24h)": round(porcentaje_norma, 1) if not np.isnan(porcentaje_norma) else "N/A",
            "Tendencia": tendencia
        })

    if not resultados:
        return pd.DataFrame()

    return pd.DataFrame(resultados)
