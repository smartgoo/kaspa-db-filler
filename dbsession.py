import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from conf import conf

_logger = logging.getLogger(__name__)

engine = create_engine(conf.DB_URI, echo=False)
Base = declarative_base()

session_maker = sessionmaker(engine)


def create_all(drop=False):
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
