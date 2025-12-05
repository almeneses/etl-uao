from sqlalchemy import create_engine

from etl.config import DB_URL, engine
from etl.models import Base


def init_db():
    Base.metadata.create_all(engine)
    print("Base de datos inicializada correctamente.")

if __name__ == "__main__":
    init_db()
