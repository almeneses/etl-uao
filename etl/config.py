import os
import sys
import boto3

from sqlalchemy import create_engine

def get_param(name, decrypt=False):
    ssm = boto3.client("ssm", region_name="us-east-2")
    param = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return param["Parameter"]["Value"]

def get_db_url():
    host = get_param("/etl/db/host")
    port = get_param("/etl/db/port")
    dbname = get_param("/etl/db/name")
    user = get_param("/etl/db/user")
    password = get_param("/etl/db/password", decrypt=True)

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


# Rutas base
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_DIR = os.path.join(DATA_DIR, "manual_csv")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# Mapear resource_id ↔ estación
API_ESTACIONES = {
    "6a0b0d95-a57d-48dd-b85e-43939aeb5d40": "Pance",
    "1ec66594-77a9-44e5-9ab4-251f208300de": "Ermita",
    "4bb8d25c-e07e-4ac7-9630-b0cf067410f6": "Univalle",
    "a83fcd9d-fd8e-44b1-96b4-102ed9c7326b": "Flora",
}


# Base de datos
DB_URL = get_db_url()

engine = create_engine(DB_URL, echo=False)
