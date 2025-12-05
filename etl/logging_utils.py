import logging
import os
from datetime import datetime

import boto3
import watchtower

from etl.config import LOG_DIR

LOG_GROUP = "/etl/logs"
REGION = "us-east-2"


def _ensure_file_handler(logger: logging.Logger) -> None:
    """Añade un FileHandler diario si aún no existe."""
    if any(getattr(h, "name", "") == "etl-file" for h in logger.handlers):
        return

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y-%m-%d')}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.set_name("etl-file")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)


def _ensure_cloudwatch_handler(logger: logging.Logger) -> None:
    """Añade el handler de CloudWatch si aún no existe; si falla, usa consola."""
    if any(isinstance(h, watchtower.CloudWatchLogHandler) for h in logger.handlers):
        return

    try:
        cw_handler = watchtower.CloudWatchLogHandler(
            log_group=LOG_GROUP,
            stream_name=f"etl-{datetime.now().strftime('%Y-%m-%d')}",
            boto3_client=boto3.client("logs", region_name=REGION),
        )
        logger.addHandler(cw_handler)
    except Exception as exc:
        # Fallback a consola para no interrumpir la ejecución del ETL.
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.set_name("etl-fallback")
        logger.addHandler(console_handler)
        logger.warning("No se pudo iniciar el logger de CloudWatch: %s", exc)


def get_logger() -> logging.Logger:
    """
    Retorna el logger principal del ETL configurado para CloudWatch y archivo local.
    """
    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)

    _ensure_cloudwatch_handler(logger)
    _ensure_file_handler(logger)

    logger.propagate = False
    return logger
