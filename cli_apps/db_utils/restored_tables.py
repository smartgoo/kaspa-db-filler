import logging

from sqlalchemy.sql import text

from conf.conf import conf
from dbsession import session_maker

_logger = logging.getLogger(__name__)


def create_restored_tables():
    pass

def drop_restored_tables():
    pass

def truncate_restored_blocks():
    with session_maker() as s:
        s.execute(text("truncate table restored_blocks;"))
        s.commit()

def truncate_restored_transactions():
    _logger.info('Truncating restored_transactions tables')
    with session_maker() as s:
        s.execute(text("truncate table restored_transactions;"))
        s.commit()

def truncate_restored_transaction_inputs():
    _logger.info('Truncating restored_transactions_inputs tables')
    with session_maker() as s:
        s.execute(text("truncate table restored_transactions_inputs;"))
        s.commit()

def truncate_restored_transaction_outputs():
    _logger.info('Truncating restored_transactions_outputs tables')
    with session_maker() as s:
        s.execute(text("truncate table restored_transactions_outputs;"))
        s.commit()

def truncate_all_restored_tables():
    _logger.info('Truncating all restored_* tables')
    with session_maker() as s:
        s.execute(text("truncate table restored_blocks, restored_transactions, restored_transactions_inputs, restored_transactions_outputs;"))
        s.commit()