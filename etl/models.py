from sqlalchemy import (Boolean, Column, Date, DateTime, Float, ForeignKey,
                        Integer, Numeric, String, Text, UniqueConstraint, func)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# Tabla: Fuente
class Fuente(Base):
    __tablename__ = "fuente"

    id_fuente = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(50), nullable=False)
    tipo = Column(String(20))
    url_api = Column(Text)
    fecha_actualizacion = Column(DateTime)
    estaciones = relationship("Estacion", back_populates="fuente")

    def __repr__(self):
        return f"<Fuente(nombre='{self.nombre}', tipo='{self.tipo}')>"


# Tabla: Estacion
class Estacion(Base):
    __tablename__ = "estacion"

    id_estacion = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(50), nullable=False, unique=True)
    nombre = Column(String(100))
    municipio = Column(String(100))
    departamento = Column(String(100))
    latitud = Column(Numeric(9, 6))
    longitud = Column(Numeric(9, 6))
    altitud = Column(Numeric(6, 2))
    fuente_id = Column(Integer, ForeignKey("fuente.id_fuente"))
    activo = Column(Boolean, default=True)

    fuente = relationship("Fuente", back_populates="estaciones")
    mediciones = relationship("Medicion", back_populates="estacion")

    def __repr__(self):
        return f"<Estacion(nombre='{self.nombre}', codigo='{self.codigo}')>"



# Tabla: Contaminante
class Contaminante(Base):
    __tablename__ = "contaminante"

    id_contaminante = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(20), nullable=False, unique=True)
    nombre = Column(String(100))
    unidad = Column(String(20))

    mediciones = relationship("Medicion", back_populates="contaminante")

    def __repr__(self):
        return f"<Contaminante(codigo='{self.codigo}', unidad='{self.unidad}')>"



# Tabla: Tiempo
class Tiempo(Base):
    __tablename__ = "tiempo"

    id_tiempo = Column(Integer, primary_key=True, autoincrement=True)
    anio = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    dia = Column(Integer, nullable=False)
    hora = Column(Integer, nullable=False)
    fecha = Column(Date)
    fecha_hora = Column(DateTime)
    dia_semana = Column(String(15))
    nombre_mes = Column(String(15))
    trimestre = Column(Integer)

    __table_args__ = (UniqueConstraint("anio", "mes", "dia", "hora", name="uix_tiempo"),)

    mediciones = relationship("Medicion", back_populates="tiempo")

    def __repr__(self):
        return f"<Tiempo({self.fecha_hora})>"



# Tabla: Medición
class Medicion(Base):
    __tablename__ = "medicion"

    id_medicion = Column(Integer, primary_key=True, autoincrement=True)
    id_estacion = Column(Integer, ForeignKey("estacion.id_estacion"))
    id_contaminante = Column(Integer, ForeignKey("contaminante.id_contaminante"))
    id_tiempo = Column(Integer, ForeignKey("tiempo.id_tiempo"))
    valor = Column(Numeric(10, 2))

    estacion = relationship("Estacion", back_populates="mediciones")
    contaminante = relationship("Contaminante", back_populates="mediciones")
    tiempo = relationship("Tiempo", back_populates="mediciones")

    __table_args__ = (
        UniqueConstraint(
            "id_estacion", "id_contaminante", "id_tiempo",
            name="uix_medicion_unica"
        ),
    )

    def __repr__(self):
        return f"<Medicion(estacion={self.id_estacion}, contaminante={self.id_contaminante}, tiempo={self.id_tiempo}, valor={self.valor})>"


class IndiceICA(Base):
    __tablename__ = "indice_ica"

    id_ica = Column(Integer, primary_key=True, autoincrement=True)
    id_estacion = Column(Integer, ForeignKey("estacion.id_estacion"))
    id_tiempo = Column(Integer, ForeignKey("tiempo.id_tiempo"))
    id_contaminante = Column(Integer, ForeignKey("contaminante.id_contaminante"))

    ica = Column(Numeric(10, 2), nullable=False)
    categoria = Column(String(30))
    fuente_calculo = Column(String(30), default="Automático")

    # Relaciones
    estacion = relationship("Estacion")
    tiempo = relationship("Tiempo")
    contaminante = relationship("Contaminante")

    __table_args__ = (
        UniqueConstraint("id_estacion", "id_tiempo", name="uix_ica_unico"),
    )

    def __repr__(self):
        return f"<IndiceICA(estacion={self.id_estacion}, tiempo={self.id_tiempo}, ica={self.ica})>"


# Tabla: Log del proceso ETL (etl_log)
class ETLLog(Base):
    __tablename__ = "etl_log"

    id_log = Column(Integer, primary_key=True, autoincrement=True)
    fecha_ejecucion = Column(DateTime, default=func.now())
    fuente = Column(String(50))
    registros_insertados = Column(Integer)
    registros_omitidos = Column(Integer)
    duracion_segundos = Column(Float)
    estado = Column(String(20))
    mensaje = Column(Text)

    def __repr__(self):
        return f"<ETLLog({self.fecha_ejecucion}, estado={self.estado})>"
