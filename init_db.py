import os
from sqlalchemy import create_engine

from etl.config import DB_URL, DB_PATH
from etl.models import Base


def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("Base de datos inicializada correctamente.")

if __name__ == "__main__":
    init_db()
