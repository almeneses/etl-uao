import os
from datetime import datetime

import pandas as pd
import requests

from .config import API_ESTACIONES, CSV_DIR, LOG_DIR
from .etl_utils import log_message

TIEMPO_BASE_EXCEL = "1899-12-30"

def discover_csv_files(base_dir=CSV_DIR):
    """
    Recorre recursivamente las subcarpetas dentro de manual_csv/
    y retorna la ruta completa de cada archivo CSV encontrado.
    """
    csv_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files


def extract_csv(file_path):
    """
    Lee un archivo CSV con estructura tipo:
    Estacion | Fecha inicial | Fecha final | <Componente>
    y devuelve un DataFrame con formato homogéneo:
    estacion | fecha_inicial | fecha_final | componente | valor | anio
    """
    try:
        log_message(f"Extrayendo archivo: {file_path}", LOG_DIR)
        df = pd.read_csv(file_path)

        # Validar estructura mínima
        expected_cols = {"Estacion", "Fecha inicial", "Fecha final"}
        if not expected_cols.issubset(df.columns):
            log_message(f"Archivo {file_path} no tiene estructura esperada", LOG_DIR)
            return None

        # Identificar componente (última columna numérica o diferente de las conocidas)
        known_cols = ["Estacion", "Fecha inicial", "Fecha final"]
        componente_cols = [c for c in df.columns if c not in known_cols]
        if len(componente_cols) != 1:
            log_message(f"No se pudo identificar un único componente en {file_path}", LOG_DIR)
            return None
        componente = componente_cols[0].strip().upper()

        # Normalizar nombres
        df = df.rename(columns={
            "Estacion": "estacion",
            "Fecha inicial": "fecha_inicial",
            "Fecha final": "fecha_final",
            componente: "valor"
        })
        df["componente"] = componente

        # Convertir fechas
        for col in ["fecha_inicial", "fecha_final"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        # Inferir año desde la primera fecha válida
        df["anio"] = df["fecha_inicial"].dt.year.fillna(method="ffill").fillna(method="bfill").astype(int)

        # Limpiar datos
        df = df.dropna(subset=["valor", "fecha_inicial", "fecha_final"])
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        log_message(f"Archivo leído correctamente: {len(df)} registros, componente: {componente}", LOG_DIR)
        return df

    except Exception as e:
        log_message(f"Error procesando {file_path}: {e}", LOG_DIR)
        return None


def extract_all():
    """
    Función principal: recorre todos los CSV dentro de manual_csv/
    y devuelve un DataFrame combinado de todos los archivos válidos.
    """
    all_files = discover_csv_files()
    log_message(f"Archivos CSV encontrados: {len(all_files)}", LOG_DIR)

    all_data = []
    for path in all_files:
        df = extract_csv(path)
        if df is not None and not df.empty:
            all_data.append(df)

    if not all_data:
        log_message("No se extrajeron datos válidos.", LOG_DIR)
        return pd.DataFrame()

    combined_df = pd.concat(all_data, ignore_index=True)
    log_message(f"Total registros combinados extraídos: {len(combined_df)}", LOG_DIR)
    return combined_df


def extract_from_api(resource_id, limit=8160, fecha_inicio=None) -> pd.DataFrame:
    """
    Extrae y normaliza datos desde la API de Datos Abiertos de Cali (CKAN).

    Parámetros:
    -----------
    resource_id : str
        ID del recurso (dataset) en CKAN.
    limit : int
        Número máximo de registros a obtener.

    Retorna:
    --------
    DataFrame con columnas:
    estacion | fecha_hora | componente | valor | anio
    """
    url_base = "https://datos.cali.gov.co/api/3/action/datastore_search"
    estacion_nombre = API_ESTACIONES.get(resource_id, "Desconocida")
    params = {"resource_id": resource_id, "limit": limit, "sort": "Fecha & Hora desc"}

    if fecha_inicio:
        params["filters"] = f'{{"Fecha & Hora": {{"$gte": "{fecha_inicio}"}}}}'
        log_message(f"Extrayendo datos desde la API con filtro a partir de {fecha_inicio}", LOG_DIR)
    else:
        log_message("Extrayendo datos completos desde la API...", LOG_DIR)


    try:
        log_message(f"Extrayendo datos desde API ({estacion_nombre})...", LOG_DIR)

        # Solicitud HTTP
        response = requests.get(url_base, params=params)
        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            log_message(f"Error: la API no retornó resultados exitosamente para {estacion_nombre}", LOG_DIR)
            return pd.DataFrame()

        records = data["result"]["records"]
        if not records:
            log_message(f"No se encontraron registros para {estacion_nombre}.", LOG_DIR)
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df.columns = [c.strip().replace("  ", " ") for c in df.columns]

        # Detectar columna temporal
        time_col = next((c for c in df.columns if "Fecha" in c and "Hora" in c), None)
        if not time_col:
            log_message(f"No se encontró columna de tiempo en {estacion_nombre}.", LOG_DIR)
            return pd.DataFrame()

        # Convertir columna temporal
        df["fecha_hora"] = pd.to_datetime(df[time_col], errors="coerce")
        
        # Si falló (NaT) y hay valores tipo numérico estilo Excel
        if df["fecha_hora"].isna().sum() > 0:
            try:
                df["fecha_hora"] = (
                    pd.to_datetime(TIEMPO_BASE_EXCEL)
                    + pd.to_timedelta(
                        df[time_col]
                        .astype(str)
                        .str.replace(",", ".", regex=False)
                        .astype(float),
                        unit="D"
                    )
                )

            except Exception as e:
                log_message(f"Error convirtiendo fechas tipo Excel: {e}", LOG_DIR)

        valid_dates = df["fecha_hora"].notna().sum()
        log_message(f"Fechas válidas detectadas: {valid_dates}", LOG_DIR)

        # Detectar columnas numéricas
        exclude = ["_id", "fecha_hora", time_col]
        value_cols = [c for c in df.columns if c not in exclude]
        log_message(f"Columnas numéricas detectadas: {value_cols}", LOG_DIR)

        # Limpieza y conversión a numérico
        for c in value_cols:
            df[c] = df[c].astype(str)
            df[c] = df[c].replace(["ND", "nan", "None", ""], None)
            df[c] = df[c].str.replace(",", ".", regex=False)
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Despivotar contaminantes
        df_melt = df.melt(
            id_vars=["fecha_hora"],
            value_vars=value_cols,
            var_name="componente",
            value_name="valor"
        )

        before_drop = len(df_melt)
        df_melt = df_melt.dropna(subset=["valor", "fecha_hora"])
        after_drop = len(df_melt)
        log_message(f"Registros antes del dropna: {before_drop}, después: {after_drop}", LOG_DIR)

        # Normalizar y agregar metadatos
        df_melt["componente"] = df_melt["componente"].str.strip()
        df_melt["anio"] = df_melt["fecha_hora"].dt.year
        df_melt["estacion"] = estacion_nombre

        df_final = df_melt[["estacion", "fecha_hora", "componente", "valor", "anio"]]
        log_message(f"Datos API ({estacion_nombre}) procesados: {len(df_final)} registros válidos.", LOG_DIR)

        return df_final

    except Exception as e:
        log_message(f"Error extrayendo datos desde API ({estacion_nombre}): {e}", LOG_DIR)
        return pd.DataFrame()
    