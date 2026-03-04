from decimal import Decimal
from typing import Optional
from uuid import UUID as PythonUUID

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import CITEXT, UUID as PostgresUUID



class Base(DeclarativeBase):
    pass


class MapRawRegion(Base):
    __tablename__ = 'map_raw_region_id_region'

    id_mapping: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_region: Mapped[str] = mapped_column(CITEXT, nullable=False, unique=True)
    id_region: Mapped[Optional[int]] = mapped_column(Integer)


class MapRawClient(Base):
    __tablename__ = 'map_client_name_to_id'

    id_mapping: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_client_name: Mapped[Optional[str]] = mapped_column(CITEXT, nullable=True, unique=True)
    id_client: Mapped[int] = mapped_column(Integer, nullable=True)

class DimAddress(Base):
    __tablename__ = 'dim_address'
    __table_args__ = {'schema': 'dev'}

    id_fias: Mapped[PythonUUID] = mapped_column(PostgresUUID, primary_key=True)
    address_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    id_region: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class RawPharmacy(Base):
    __tablename__ = 'raw_pharmacy_external'
    __table_args__ = {'schema': 'dev'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_pharmacy_name: Mapped[str] = mapped_column(CITEXT, nullable=False, unique=True)
    id_pharmacy: Mapped[int] = mapped_column(Integer, nullable=True)
    id_client: Mapped[int] = mapped_column(Integer, nullable=True)
