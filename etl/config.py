import os

from sqlalchemy import create_engine

# Rutas base
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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


# Base de datos SQLite
DB_PATH = os.path.join(DATA_DIR, "etl_database.db")
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DB_URL, echo=False)
