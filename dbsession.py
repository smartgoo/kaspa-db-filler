import logging
import os
import asyncio

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

from conf.conf import conf

_logger = logging.getLogger(__name__)

Base = declarative_base()

engine = create_engine(conf.DB_URI, echo=False)
session_maker = sessionmaker(engine)

async_engine = create_async_engine(conf.ASYNC_DB_URI, echo=False)
async_session_maker = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

def create_all(drop=False):
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

async def async_create_all(drop=False):
    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)