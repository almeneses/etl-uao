import os
import sys
import time

import pandas as pd

from etl.config import API_ESTACIONES, CSV_DIR, LOG_DIR, PROCESSED_DIR
from etl.etl_logger import log_etl_run
from etl.etl_utils import log_message, hay_datos_nuevos
from etl.extract import discover_csv_files, extract_all, extract_from_api
from etl.ica_calculator import calcular_indice_ica
from etl.load import load_to_db
from etl.load_ica import load_to_ica_database
from etl.transform import transform_data


def run_etl_api():
    log_message("=== INICIO DEL PROCESO ETL ===", LOG_DIR)

    for resource_id, nombre_estacion in API_ESTACIONES.items():
        start_time = time.time()
        estado = "Éxito"
        mensaje = "Proceso finalizado correctamente"
        registros_insertados = 0
        registros_omitidos = 0

        try:
            log_message(f"Iniciando ETL para {nombre_estacion}", LOG_DIR)
            
            hay_nuevos, ultima_fecha_bd = hay_datos_nuevos(nombre_estacion, resource_id)

            if not hay_nuevos:
                estado = "Sin datos nuevos"
                mensaje = f"No se encontraron datos más recientes para {nombre_estacion}."
                log_message(mensaje, LOG_DIR)
            else:
                log_message(f"Nuevos datos detectados para {nombre_estacion}. Ejecutando ETL...", LOG_DIR)
                
                fecha_inicio = (
                    ultima_fecha_bd.strftime("%Y-%m-%dT%H:%M:%S")
                    if ultima_fecha_bd is not None
                    else None
                )

                df_api = extract_from_api(resource_id, limit=8160*4, fecha_inicio=fecha_inicio)
                df_clean = transform_data(df_api)
                load_to_db(df_clean)
                load_to_ica_database(calcular_indice_ica(df_clean))
                registros_insertados = len(df_clean)

        except Exception as e:
            estado = "Error"
            mensaje = str(e)
            log_message(f"Error en ETL de {nombre_estacion}: {e}", LOG_DIR)

        finally:
            duracion = round(time.time() - start_time, 2)
            log_etl_run(
                fuente=nombre_estacion,
                registros_insertados=registros_insertados,
                registros_omitidos=registros_omitidos,
                duracion=duracion,
                estado=estado,
                mensaje=mensaje,
            )

    log_message("=== FIN DEL PROCESO ETL ===", LOG_DIR)



if __name__ == "__main__":
    # Garantizar que los imports funcionen correctamente
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    run_etl_api()