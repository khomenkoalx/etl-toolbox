from datetime import datetime
from typing import Optional, List
from uuid import UUID

from sqlalchemy import and_
from sqlalchemy import select, func, update
from sqlalchemy.orm import Session
from contextlib import contextmanager

from core.db import SessionLocal, engine
from core.models import MapRawRegion, MapRawClient, DimAddress, RawPharmacy


@contextmanager
def get_db_session():
    """Контекстный менеджер для сессий БД"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class Mapper:

    def __init__(self):
        self._region_cache = {}
        self._client_cache = {}

    def _get_region(self, raw_region: str, session: Session) -> Optional[MapRawRegion]:
        stmt = select(MapRawRegion).where(
            MapRawRegion.raw_region == raw_region
        )
        result = session.execute(stmt)
        return result.scalar_one_or_none()
    
    def _get_client(self, raw_client_name: str, session: Session) -> Optional[MapRawClient]:
        stmt = select(MapRawClient).where(
            MapRawClient.raw_client_name == raw_client_name
        )
        result = session.execute(stmt)
        return result.scalar_one_or_none()

    def get_or_create_id_region(self, raw_region: str) -> Optional[int]:
        if raw_region in self._region_cache:
            return self._region_cache[raw_region]

        with get_db_session() as session:
            existing = self._get_region(raw_region, session)

            if existing:
                result = existing.id_region
            else:
                new_region = MapRawRegion(
                    raw_region=raw_region,
                    id_region=None
                )
                session.add(new_region)
                # Коммит будет выполнен в контекстном менеджере
                result = None

            self._region_cache[raw_region] = result
            return result

    def update_id_of_region(self, raw_region: str, id: int) -> bool:
        with get_db_session() as session:
            region = self._get_region(raw_region, session)

            if region:
                region.id_region = id
                return True
        return False

    def get_or_create_id_client(
            self,
            raw_client_name: str,
            id_client: Optional[int] = None  # Изменен тип на int
    ) -> Optional[int]:

        if raw_client_name in self._client_cache:
            return self._client_cache[raw_client_name]

        with get_db_session() as session:
            existing = self._get_client(raw_client_name, session)
            
            # Получаем максимальный id_client
            max_result = session.execute(select(func.max(MapRawClient.id_client))).scalar()
            
            # Если id_client не передан, вычисляем новый
            if not id_client:
                # Обработка случая, когда таблица пустая (max_result = None)
                current_max = max_result if max_result is not None else 0
                id_client = max(current_max + 1, 30000)
            
            if existing:
                result = existing.id_client
            else:
                new_client = MapRawClient(
                    raw_client_name=raw_client_name,
                    id_client=id_client
                )
                session.add(new_client)
                # Коммит будет выполнен в контекстном менеджере
                result = None

            self._client_cache[raw_client_name] = result
            return result
        
    def update_id_of_client(self, raw_client_name: str, id: int) -> bool:
        with get_db_session() as session:
            client = self._get_client(raw_client_name, session)

            if client:
                client.id_client = id
                return True
        return False

class AddressCRUD:
    
    def add_new_fias(self, id_fias: UUID) -> bool:
        if self.get_address_by_fias(id_fias):
            return True
        with get_db_session() as session:
            new_obj = DimAddress(id_fias=id_fias)
            session.add(new_obj)
            return True

    def get_address_by_fias(self, id_fias: UUID) -> Optional[DimAddress]:
        """Получить адрес по id_fias"""
        with get_db_session() as session:
            stmt = select(DimAddress).where(DimAddress.id_fias == id_fias)
            return session.execute(stmt).scalar_one_or_none()
    
    def get_addresses_with_missing_data(self) -> List[DimAddress]:
        """Получить адреса с незаполненными данными"""
        with get_db_session() as session:
            stmt = select(DimAddress).where(
                (DimAddress.address_name.is_(None)) |
                (DimAddress.address_name == '') |
                (DimAddress.id_region.is_(None)) |
                (DimAddress.city.is_(None)) |
                (DimAddress.city == '')
            )
            return list(session.execute(stmt).scalars().all())
    
    def update_address(self, id_fias: UUID, **kwargs) -> bool:
        """Обновить данные адреса"""
        with get_db_session() as session:
            stmt = update(DimAddress).where(DimAddress.id_fias == id_fias).values(**kwargs)
            return session.execute(stmt).rowcount > 0


class RawPharmacyCRUD:
    def __init__(self):
        self.raw_address_cache = {}

    def get_id_pharmacy(self, raw_pharmacy_name: str, id_client: int) -> Optional[int]:
        with get_db_session() as session:
            if (raw_pharmacy_name, id_client) in self.raw_address_cache:
                return self.raw_address_cache[(raw_pharmacy_name, id_client)]
            stmt = select(RawPharmacy).where(
                and_(
                    RawPharmacy.raw_pharmacy_name == raw_pharmacy_name,
                    RawPharmacy.id_client == id_client
                )
            )
            result = session.execute(stmt).scalar_one_or_none()
            if result:
                self.raw_address_cache[(raw_pharmacy_name, id_client)] = result.id_pharmacy
                return result.id_pharmacy
            else:
                self.raw_address_cache[(raw_pharmacy_name, id_client)] = None


mapper = Mapper()
address_crud = AddressCRUD()
raw_pharmacy_crud = RawPharmacyCRUD()
