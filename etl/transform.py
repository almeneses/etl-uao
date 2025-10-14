import pandas as pd
import re
from .etl_utils import log_message
from .config import LOG_DIR

def _separar_nombre_unidad(texto):
    if pd.isna(texto):
        return (None, None)
    
    texto = str(texto).strip()
    # Extraer datos entre parentesis
    match = re.search(r"\((.*?)\)", texto)
    unidad = match.group(1) if match else None
    nombre = re.sub(r"\s*\(.*?\)", "", texto).strip()

    return (nombre, unidad)

def separar_nombre_y_unidad(df):
    """
    Retorna una tupla (nombre, unidad)
    Ejemplo: 'O3 (ug/m3)' -> ('O3', 'ug/m3')
    """
    df[["componente", "unidad"]] = df["componente"].apply(lambda x: pd.Series(_separar_nombre_unidad(x)))

    return df

def imputar_datos_faltantes(df_mediciones: pd.DataFrame) -> pd.DataFrame:
    """
    Imputa valores faltantes en el DataFrame original sin modificar su estructura.
    Interpola solo dentro de huecos menores o iguales a 5 días (120 horas).
    Para huecos mayores, mantiene los valores NaN.

    Parámetros:
    -----------
    df_mediciones : pd.DataFrame
        Debe contener:
        - estacion
        - fecha_hora (datetime)
        - componente
        - valor (numérico)

    Retorna:
    --------
    pd.DataFrame con los valores imputados en la misma columna 'valor'.
    """
    log_message("Iniciando imputación de datos faltantes...", LOG_DIR)

    df_resultado = []
    for (nombre_estacion, nombre_componente), df_grupo in df_mediciones.groupby(["estacion", "componente"]):
        df_grupo = df_grupo.copy().sort_values("fecha_hora")
        df_grupo["fecha_hora"] = pd.to_datetime(df_grupo["fecha_hora"])

        # Asegurar tipo numérico y orden correcto
        df_grupo["valor"] = pd.to_numeric(df_grupo["valor"], errors="coerce")

        # Interpolación temporal
        df_grupo["valor"] = df_grupo.set_index("fecha_hora")["valor"].interpolate(
            method="time", limit=12000, limit_direction="both"
        ).values

        df_resultado.append(df_grupo)

    df_final = pd.concat(df_resultado, ignore_index=True)
    log_message("Imputación de valores faltantes completada correctamente.", LOG_DIR)
    
    return df_final

def transform_data(df):
    log_message("Iniciando transformación de datos (API)...", LOG_DIR)
    initial_len = len(df)

    df = df.drop_duplicates(subset=["estacion", "fecha_hora", "componente"])
    df = df.dropna(subset=["valor", "fecha_hora"])
    df = separar_nombre_y_unidad(df)
    df = imputar_datos_faltantes(df) 
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    
    log_message(f"Registros antes: {initial_len}, después de limpiar: {len(df)}", LOG_DIR)
    
    return df

