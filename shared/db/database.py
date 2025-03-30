# shared/db/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/iot_config")

engine = create_async_engine(DATABASE_URL, echo=bool(os.getenv("SQL_DEBUG", False)))
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_db_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

# Optional: Function to create tables (run once during setup)
# from shared.models.db_models import Base
# async def init_db():
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all) # Be careful in prod!
#         await conn.run_sync(Base.metadata.create_all)