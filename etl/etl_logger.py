from sqlalchemy.orm import sessionmaker

from etl.config import LOG_DIR, engine
from etl.etl_utils import log_message
from etl.models import ETLLog

Session = sessionmaker(bind=engine)


def log_etl_run(
    fuente: str,
    registros_insertados: int,
    registros_omitidos: int,
    duracion: float,
    estado: str = "Éxito",
    mensaje: str = "Proceso completado correctamente"
):
    """
    Registra una ejecución del proceso ETL en la tabla etl_log.

    Parámetros:
    -----------
    fuente : str
        Nombre de la estación o fuente procesada.
    registros_insertados : int
        Número de registros insertados en la base de datos.
    registros_omitidos : int
        Número de registros omitidos (duplicados o ya existentes).
    duracion : float
        Tiempo total de ejecución en segundos.
    estado : str
        Estado del proceso ("Éxito", "Error", "Sin datos", etc.).
    mensaje : str
        Mensaje descriptivo o detalle del proceso.

    Retorna:
    --------
    bool : True si el log se registró correctamente, False si hubo error.
    """
    session = Session()
    try:
        nuevo_log = ETLLog(
            fuente=fuente,
            registros_insertados=registros_insertados,
            registros_omitidos=registros_omitidos,
            duracion_segundos=duracion,
            estado=estado,
            mensaje=mensaje,
        )
        session.add(nuevo_log)
        session.commit()

        log_message(
            f"Log registrado -> Fuente: {fuente}, Estado: {estado}, Duración: {duracion}s",
            LOG_DIR,
        )
        return True

    except Exception as e:
        session.rollback()
        log_message(f"Error al registrar log ETL: {e}", LOG_DIR)
        return False

    finally:
        session.close()
