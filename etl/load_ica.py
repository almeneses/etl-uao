import pandas as pd
from sqlalchemy.orm import sessionmaker

from etl.config import LOG_DIR, engine
from etl.etl_utils import log_message
from etl.models import Contaminante, Estacion, IndiceICA, Tiempo

Session = sessionmaker(bind=engine)

def load_to_ica_database(df_ica):
    """
    Inserta los resultados del cálculo del ICA en la tabla 'indice_ica'.

    Parámetros:
    -----------
    df_ica : pd.DataFrame
        DataFrame con columnas:
        - estacion
        - id_fecha
        - ica
        - categoria
        - contaminante_dominante
        - fuente_calculo
    """
    session = Session()
    try:
        for _, fila in df_ica.iterrows():
            estacion = session.query(Estacion).filter_by(nombre=fila["estacion"]).first()
            contaminante = session.query(Contaminante).filter_by(nombre=fila["contaminante_dominante"]).first()
            fecha_hora = pd.to_datetime(fila["fecha_hora"])

            # Obtener o crear registro de tiempo
            tiempo = session.query(Tiempo).filter_by(
                anio=fecha_hora.year,
                mes=fecha_hora.month,
                dia=fecha_hora.day,
                hora=fecha_hora.hour
            ).first()

            if not tiempo:
                tiempo = Tiempo(
                    anio=fecha_hora.year,
                    mes=fecha_hora.month,
                    dia=fecha_hora.day,
                    hora=fecha_hora.hour,
                    fecha=fecha_hora.date(),
                    fecha_hora=fecha_hora
                )
                session.add(tiempo)
                session.flush()

            # Evitar duplicados
            existe = session.query(IndiceICA).filter_by(
                id_estacion=estacion.id_estacion,
                id_tiempo=tiempo.id_tiempo
            ).first()

            if not existe:
                nuevo_ica = IndiceICA(
                    id_estacion=estacion.id_estacion,
                    id_tiempo=tiempo.id_tiempo,
                    id_contaminante=contaminante.id_contaminante,
                    ica=fila["ica"],
                    categoria=fila["categoria"],
                    fuente_calculo=fila["fuente_calculo"]
                )
                session.add(nuevo_ica)

        session.commit()
    except Exception as e:
        session.rollback()
        log_message(f"Error cargando ICA: {e}", LOG_DIR)
    finally:
        session.close()
