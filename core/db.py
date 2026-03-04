from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from core.config import settings


engine = create_engine(
    settings.db_connection_string_secret,
    echo=False
)

SessionLocal = sessionmaker(
    bind=engine,
    class_=Session,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)


def get_db():
    """Функция для получения сессии БД (для использования в зависимостях)"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
