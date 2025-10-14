import datetime
import os

import pandas as pd
import requests
from sqlalchemy.orm import Session

from etl.config import LOG_DIR, engine
from etl.models import Estacion, Medicion, Tiempo


def log_message(message, log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"etl_{datetime.date.today()}.log")
    with open(log_file, "a") as f:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")
    print(message)

def ultima_fecha_api(resource_id: str) -> pd.Timestamp | None:
    """
    Consulta la API de Datos Abiertos (CKAN) para obtener la fecha más reciente disponible
    para un recurso específico, descargando solo un registro.
    """
    url_base = "https://datos.cali.gov.co/api/3/action/datastore_search"
    params = {"resource_id": resource_id, "limit": 1, "sort": "Fecha & Hora desc"}

    try:
        response = requests.get(url_base, params=params)
        response.raise_for_status()
        data = response.json()

        if not data.get("success") or not data["result"]["records"]:
            return None

        registro = data["result"]["records"][0]
        fecha_str = registro.get("Fecha & Hora")
        if not fecha_str:
            return None

        return pd.to_datetime(fecha_str, errors="coerce")

    except Exception as e:
        log_message(f"Error al consultar la última fecha de la API ({resource_id}): {e}", LOG_DIR)
        return None


def hay_datos_nuevos(nombre_estacion: str, resource_id: str):
    """
    Compara la última fecha en la BD (tabla medicion/tiempo)
    con la última fecha disponible en la API.
    Retorna una tupla (hay_nuevos: bool, ultima_fecha_bd: pd.Timestamp | None)
    """
    session = Session(engine)
    try:
        estacion = session.query(Estacion).filter_by(nombre=nombre_estacion).first()
        if not estacion:
            return True, None

        ultima_medicion = (
            session.query(Tiempo.fecha_hora)
            .join(Medicion, Medicion.id_tiempo == Tiempo.id_tiempo)
            .filter(Medicion.id_estacion == estacion.id_estacion)
            .order_by(Tiempo.fecha_hora.desc())
            .limit(1)
            .scalar()
        )

        ultima_api = ultima_fecha_api(resource_id)
        if ultima_api is None:
            return False, ultima_medicion

        if ultima_medicion is None:
            return True, None

        fecha_bd = pd.to_datetime(ultima_medicion)
        hay_nuevos = ultima_api > fecha_bd
        
        log_message(
            f"Última fecha en BD para {nombre_estacion}: {fecha_bd} | Última en API: {ultima_api} | Nuevos: {hay_nuevos}",
            LOG_DIR,
        )

        return hay_nuevos, ultima_medicion

    except Exception as e:
        log_message(f"Error verificando nuevos datos para {nombre_estacion}: {e}", LOG_DIR)
        return False

    finally:
        session.close()