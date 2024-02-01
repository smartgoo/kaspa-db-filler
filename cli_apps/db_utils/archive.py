from datetime import datetime, timedelta
import logging

from sqlalchemy import asc

from conf.conf import conf
from dbsession import session_maker
from models.Block import Block
from models.Transaction import Transaction

from .Archiver import Archiver

_logger = logging.getLogger(__name__)

def main(del_pg: bool = False, del_csvs: bool = False):
    # Get oldest block date
    with session_maker() as s:
        oldest_block = s.query(Block).order_by(asc(Block.timestamp)).first() 
        oldest_tx = s.query(Transaction).order_by(asc(Transaction.block_time)).first()

    today = datetime.now().date()

    # Always assume (naively) target date is the day after oldest block timestamp in DB
    target_date = oldest_block.timestamp.date() + timedelta(days=1)

    # Iterate days between target_date and yesterday (keep ~2 days of data in db)
    days_archived = 0
    while target_date < (today - timedelta(days=1)):
        # Convert target_date to datetime
        target_datetime = datetime(
            year=target_date.year, 
            month=target_date.month, 
            day=target_date.day
        )

        # Archive will include data from <23:50:00 day before target_date> to <00:10:00 day after target_date>
        padding_mins = 10
        export_point_datetime = target_datetime + timedelta(days=1, minutes=padding_mins)
        
        # BUT it only deletes anything older than <23:50:00 of target date>. So that when we archive next day, we can honor the 10 min padding 
        delete_point_datetime = target_datetime + timedelta(hours=23, minutes=60-padding_mins)

        _logger.info(f'today = {today}')
        _logger.info(f'Processing target_date {target_date}')
        _logger.info(f'oldest_block.timestamp = {oldest_block.timestamp}')
        _logger.info(f'oldest_tx.block_time = {datetime.fromtimestamp(oldest_tx.block_time / 1000)}')
        _logger.info(f'export_point_datetime = {export_point_datetime}')
        _logger.info(f'delete_point_datetime = {delete_point_datetime}')

        # Init and run archiver for the given target date
        archiver = Archiver(
            dir_name=f'{target_date}_archive',
            export_point_datetime=export_point_datetime,
            delete_point_datetime=delete_point_datetime
        )
        archiver.run(
            del_pg=del_pg,
            del_local_dir=del_csvs, 
            del_local_gz=False
        )

        target_date += timedelta(days=1)
        days_archived += 1

    _logger.info(f'Days archived = {days_archived}')

    
