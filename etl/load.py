import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from etl.config import LOG_DIR, engine
from etl.etl_utils import log_message
from etl.models import Contaminante, Estacion, Medicion, Tiempo

Session = sessionmaker(bind=engine)

# Funciones auxiliares para cargar dimensiones
def get_or_create_estacion(session, nombre_estacion):
    """Obtiene o crea una estación."""
    estacion = session.query(Estacion).filter_by(nombre=nombre_estacion).first()
    if not estacion:
        estacion = Estacion(
            nombre=nombre_estacion,
            codigo=nombre_estacion.upper(),
            activo=True
        )
        session.add(estacion)
        session.flush()  # Permite obtener su ID sin commit
        log_message(f"Nueva estación registrada: {nombre_estacion}", LOG_DIR)
    return estacion


def get_or_create_contaminante(session, nombre_componente, unidad_componente):
    """Obtiene o crea un contaminante."""
    contaminante = session.query(Contaminante).filter_by(nombre=nombre_componente).first()
    if not contaminante:
        contaminante = Contaminante(
            nombre=nombre_componente,
            codigo=nombre_componente,
            unidad=unidad_componente
        )
        session.add(contaminante)
        session.flush()
        log_message(f"Nuevo contaminante registrado: {nombre_componente}", LOG_DIR)
    return contaminante


def get_or_create_tiempo(session, fecha_hora):
    """Obtiene o crea un registro de tiempo."""
    fecha_hora = pd.to_datetime(fecha_hora)
    anio, mes, dia, hora = fecha_hora.year, fecha_hora.month, fecha_hora.day, fecha_hora.hour

    tiempo = session.query(Tiempo).filter_by(
        anio=anio, mes=mes, dia=dia, hora=hora
    ).first()

    if not tiempo:
        tiempo = Tiempo(
            anio=anio,
            mes=mes,
            dia=dia,
            hora=hora,
            fecha=fecha_hora.date(),
            fecha_hora=fecha_hora,
            dia_semana=fecha_hora.strftime("%A"),
            nombre_mes=fecha_hora.strftime("%B"),
            trimestre=(mes - 1) // 3 + 1
        )
        session.add(tiempo)
        session.flush()
    return tiempo


# Función principal para carga de datos
def load_to_db(df):
    """
    Carga los datos del DataFrame en las tablas:
    - estacion
    - contaminante
    - tiempo
    - medicion
    """
    if df.empty:
        log_message("No se recibieron datos para cargar en la base de datos.", LOG_DIR)
        return

    session = Session()

    try:
        log_message("Iniciando carga de datos en la base de datos...", LOG_DIR)

        registros_insertados = 0
        registros_omitidos = 0

        for _, row in df.iterrows():
            # Obtener claves foráneas
            estacion = get_or_create_estacion(session, row["estacion"])
            contaminante = get_or_create_contaminante(session, row["componente"], row["unidad"])
            tiempo = get_or_create_tiempo(session, row["fecha_hora"])

            existe = session.query(Medicion).filter_by(
                id_estacion=estacion.id_estacion,
                id_contaminante=contaminante.id_contaminante,
                id_tiempo=tiempo.id_tiempo
            ).first()

            if existe:
                registros_omitidos += 1
                continue

            nueva_medicion = Medicion(
                id_estacion=estacion.id_estacion,
                id_contaminante=contaminante.id_contaminante,
                id_tiempo=tiempo.id_tiempo,
                valor=row["valor"]
            )
            session.add(nueva_medicion)
            registros_insertados += 1

        session.commit()
        log_message(f"Carga completada: {registros_insertados} insertados, {registros_omitidos} omitidos.", LOG_DIR)

    except Exception as e:
        session.rollback()
        log_message(f"Error en la carga: {e}", LOG_DIR)

    finally:
        session.close()
