import logging
from datetime import datetime

import boto3
import watchtower

# Configura un logger compartido que escribe en CloudWatch.
LOG_GROUP = "/dashboard/logs"
REGION = "us-east-2"


def get_logger() -> logging.Logger:
    """
    Devuelve un logger configurado para enviar mensajes a CloudWatch.

    Si el handler ya existe, lo reutiliza para evitar duplicados en las
    re-ejecuciones de Streamlit.
    """
    logger = logging.getLogger("dashboard")
    logger.setLevel(logging.INFO)

    handler_existente = any(
        isinstance(h, watchtower.CloudWatchLogHandler) or getattr(h, "name", "") == "dashboard-fallback"
        for h in logger.handlers
    )
    if not handler_existente:
        try:
            handler = watchtower.CloudWatchLogHandler(
                log_group=LOG_GROUP,
                stream_name=f"dashboard-{datetime.now().strftime('%Y-%m-%d')}",
                boto3_client=boto3.client("logs", region_name=REGION),
            )
            logger.addHandler(handler)
        except Exception as exc:
            # Fallback a consola para no romper el dashboard si falla CloudWatch.
            fallback = logging.StreamHandler()
            fallback.setLevel(logging.INFO)
            fallback.set_name("dashboard-fallback")
            logger.addHandler(fallback)
            logger.warning("No se pudo iniciar el logger de CloudWatch: %s", exc)

    logger.propagate = False
    return logger
