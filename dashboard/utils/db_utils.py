import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DB_URL = "sqlite:///data/etl_database.db"
DB_PATH = "etl_database.db"

engine = create_engine(DB_URL, echo=False, future=True)

def obtener_estaciones():
    query = text("""
        SELECT id_estacion, nombre
        FROM estacion
        ORDER BY nombre
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def obtener_contaminantes():
    query = text("""
        SELECT id_contaminante, nombre
        FROM contaminante
        ORDER BY nombre
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def obtener_mediciones(id_estacion, id_contaminante, fecha_inicio=None, fecha_fin=None):
    base_query = """
        SELECT t.fecha_hora, m.valor
        FROM medicion m
        JOIN tiempo t ON m.id_tiempo = t.id_tiempo
        WHERE m.id_estacion = :id_estacion
        AND m.id_contaminante = :id_contaminante
    """
    params = {"id_estacion": int(id_estacion), "id_contaminante": int(id_contaminante)}

    if fecha_inicio and fecha_fin:
        base_query += " AND t.fecha_hora BETWEEN :fecha_inicio AND :fecha_fin"
        params["fecha_inicio"] = fecha_inicio
        params["fecha_fin"] = fecha_fin

    base_query += " ORDER BY t.fecha_hora"

    with engine.connect() as conn:
        return pd.read_sql(text(base_query), conn, params=params)

@st.cache_data(ttl=300)
def obtener_indice_ica(id_estacion, fecha_inicio=None, fecha_fin=None):
    """
    Retorna el índice ICA de una estación uniendo con la tabla tiempo.
    """
    query = """
        SELECT 
            t.fecha_hora,
            i.ica,
            i.categoria,
            i.fuente_calculo
        FROM indice_ica i
        JOIN tiempo t ON i.id_tiempo = t.id_tiempo
        WHERE i.id_estacion = :id_estacion
    """
    params = {"id_estacion": int(id_estacion)}

    if fecha_inicio and fecha_fin:
        query += " AND t.fecha_hora BETWEEN :fecha_inicio AND :fecha_fin"
        params.update({"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})

    query += " ORDER BY t.fecha_hora"

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    if not df.empty:
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])

    return df

@st.cache_data(ttl=600)
def obtener_contaminantes_por_estacion(id_estacion: int) -> pd.DataFrame:
    """
    Retorna los contaminantes medidos por una estación específica.
    """
    query = """
        SELECT DISTINCT c.id_contaminante, c.nombre
        FROM medicion m
        JOIN contaminante c ON m.id_contaminante = c.id_contaminante
        WHERE m.id_estacion = :id_estacion
        ORDER BY c.nombre
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params={"id_estacion": int(id_estacion)})
