import pandas as pd

from etl.config import LOG_DIR
from etl.etl_utils import log_message

# Rangos oficiales ICA por contaminante
RANGOS_ICA = {
    "PM2.5": [
        (0.0, 12.0, 0, 50, "Buena"),
        (12.1, 35.4, 51, 100, "Moderada"),
        (35.5, 55.4, 101, 150, "Dañina a grupos sensibles"),
        (55.5, 150.4, 151, 200, "Dañina"),
        (150.5, 250.4, 201, 300, "Muy dañina"),
        (250.5, 500.4, 301, 500, "Peligrosa"),
    ],
    "PM10": [
        (0, 54, 0, 50, "Buena"),
        (55, 154, 51, 100, "Moderada"),
        (155, 254, 101, 150, "Dañina a grupos sensibles"),
        (255, 354, 151, 200, "Dañina"),
        (355, 424, 201, 300, "Muy dañina"),
        (425, 604, 301, 500, "Peligrosa"),
    ],
    "O3": [
        (0.000, 0.054, 0, 50, "Buena"),
        (0.055, 0.070, 51, 100, "Moderada"),
        (0.071, 0.085, 101, 150, "Dañina a grupos sensibles"),
        (0.086, 0.105, 151, 200, "Dañina"),
        (0.106, 0.200, 201, 300, "Muy dañina"),
    ],
    "CO": [
        (0.0, 4.4, 0, 50, "Buena"),
        (4.5, 9.4, 51, 100, "Moderada"),
        (9.5, 12.4, 101, 150, "Dañina a grupos sensibles"),
        (12.5, 15.4, 151, 200, "Dañina"),
        (15.5, 30.4, 201, 300, "Muy dañina"),
        (30.5, 50.4, 301, 500, "Peligrosa"),
    ],
    "NO2": [
        (0, 53, 0, 50, "Buena"),
        (54, 100, 51, 100, "Moderada"),
        (101, 360, 101, 150, "Dañina a grupos sensibles"),
        (361, 649, 151, 200, "Dañina"),
        (650, 1249, 201, 300, "Muy dañina"),
        (1250, 2049, 301, 500, "Peligrosa"),
    ],
    "SO2": [
        (0, 35, 0, 50, "Buena"),
        (36, 75, 51, 100, "Moderada"),
        (76, 185, 101, 150, "Dañina a grupos sensibles"),
        (186, 304, 151, 200, "Dañina"),
        (305, 604, 201, 300, "Muy dañina"),
        (605, 1004, 301, 500, "Peligrosa"),
    ],
    "H2S": [
        (0, 30, 0, 50, "Buena"),
        (31, 70, 51, 100, "Moderada"),
        (71, 150, 101, 150, "Dañina a grupos sensibles"),
        (151, 225, 151, 200, "Dañina"),
        (226, 300, 201, 300, "Muy dañina"),
        (301, 500, 301, 500, "Peligrosa"),
    ],
}


def calcular_ica(contaminante: str, concentracion: float):
    """
    Calcula el Índice de Calidad del Aire (ICA) y su categoría
    para un contaminante específico, según los rangos IDEAM/EPA.

    Parámetros:
    -----------
    contaminante : str
        Nombre del contaminante (por ejemplo: "PM2.5", "PM10", "O3").
    concentracion : float
        Valor de concentración medido.

    Retorna:
    --------
    (float, str)
        - valor_ica: valor numérico del índice ICA (redondeado)
        - categoria: texto con la categoría ("Buena", "Moderada", etc.)
    """
    contaminante = contaminante.upper()

    if contaminante not in RANGOS_ICA:
        return None, None

    rangos_contaminante = RANGOS_ICA[contaminante]

    for conc_min, conc_max, ica_min, ica_max, categoria in rangos_contaminante:
        if conc_min <= concentracion <= conc_max:
            valor_ica = ((ica_max - ica_min) / (conc_max - conc_min)) * (concentracion - conc_min) + ica_min
            return round(valor_ica, 2), categoria

    return None, None


def calcular_indice_ica(dataframe_mediciones: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el Índice ICA consolidado (por estación y hora) a partir
    de las mediciones de contaminantes.

    Parámetros:
    -----------
    dataframe_mediciones : pd.DataFrame
        Debe contener columnas:
        - estacion
        - fecha_hora
        - componente
        - valor

    Retorna:
    --------
    pd.DataFrame con columnas:
        estacion | fecha_hora | ica | categoria | contaminante_dominante | fuente_calculo
    """
    log_message("Iniciando cálculo del índice ICA...", LOG_DIR)

    resultados = []
    mediciones_agrupadas = dataframe_mediciones.groupby(["estacion", "fecha_hora"])

    for (nombre_estacion, fecha_hora), grupo_mediciones in mediciones_agrupadas:
        resultados_por_contaminante = []

        for _, fila in grupo_mediciones.iterrows():
            componente = fila["componente"].upper().replace(" ", "").replace("(UG/M3)", "")
            valor_concentracion = fila["valor"]

            valor_ica, categoria = calcular_ica(componente, valor_concentracion)

            if valor_ica is not None:
                resultados_por_contaminante.append({
                    "componente": componente,
                    "ica": valor_ica,
                    "categoria": categoria,
                })

        if resultados_por_contaminante:
            # El ICA total se determina por el contaminante con ICA más alto
            contaminante_dominante = max(resultados_por_contaminante, key=lambda x: x["ica"])
            resultados.append({
                "estacion": nombre_estacion,
                "fecha_hora": fecha_hora,
                "ica": contaminante_dominante["ica"],
                "categoria": contaminante_dominante["categoria"],
                "contaminante_dominante": contaminante_dominante["componente"],
                "fuente_calculo": "Automático",
            })

    df_ica = pd.DataFrame(resultados)
    log_message(f"Cálculo ICA completado: {len(df_ica)} registros generados.", LOG_DIR)
    return df_ica
