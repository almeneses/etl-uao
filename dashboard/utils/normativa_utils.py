from typing import Dict, Optional

import pandas as pd
from sqlalchemy import text

from .db_utils import engine

# Respaldo si no hay tabla
LIMITES_FALLBACK = {
    # contaminante -> fuente -> tipo -> valor
    'PM2,5': {'OMS': {'24h': 15.0}, 'IDEAM': {'24h': 37.0}},
    'PM10': {'OMS': {'24h': 45.0}, 'IDEAM': {'24h': 75.0}},
    'O3':   {'OMS': {'8h': 100.0}, 'IDEAM': {'8h': 100.0}},
    'CO':   {'OMS': {'8h': 10000.0}, 'IDEAM': {'8h': 10000.0}},
    'SO2':  {'OMS': {'24h': 40.0}, 'IDEAM': {'24h': 65.0}},
    'NO2':  {'OMS': {'24h': 25.0, 'anual': 10.0}, 'IDEAM': {'1h': 200.0, 'anual': 40.0}},
    'H2S':  {'OMS': {'24h': 100.0}, 'IDEAM': {'24h': 100.0}},
}

def cargar_limites_desde_bd() -> Optional[pd.DataFrame]:
    with engine.connect() as conn:
        tablas = pd.read_sql(text("SELECT name FROM sqlite_master WHERE type='table'"), conn)
        if 'limites_norma' not in tablas['name'].tolist():
            return None
        df = pd.read_sql(text("SELECT contaminante, fuente, tipo, valor, unidad FROM limites_norma"), conn)
        return df

def obtener_limites(contaminante: str) -> Dict[str, Dict[str, float]]:
    """
    Retorna dict como: {'OMS': {'24h': 15.0, ...}, 'IDEAM': {'24h': 37.0, ...}}
    Si no hay en BD, intenta fallback.
    """
    df = cargar_limites_desde_bd()
    if df is None:
        return LIMITES_FALLBACK.get(contaminante, {})

    df_c = df[df['contaminante'].str.lower() == contaminante.lower()]
    res: Dict[str, Dict[str, float]] = {}
    for _, r in df_c.iterrows():
        fuente = str(r['fuente'])
        tipo = str(r['tipo'])
        valor = float(r['valor'])
        res.setdefault(fuente, {})[tipo] = valor
    return res
