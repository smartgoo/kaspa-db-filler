from datetime import datetime, timedelta
import shutil
import tarfile
import time
import os

from sqlalchemy import asc
from sqlalchemy.sql import text

from conf import conf
from dbsession import session_maker
from models.Block import Block
from models.Transaction import (
    Transaction,
    TransactionInput,
    TransactionOutput,
)

class Archiver:
    def __init__(self, dir_name, export_point_datetime, delete_point_datetime):
        self.dir_name = dir_name

        # Format far_datetime for use in SQL
        # self.far_datetime_str = far_datetime.strftime('%Y-%m-%d %H:%M:%S')
        # self.far_timestamp = int(far_datetime.timestamp() * 1000)

        # Format export_point_datetime for use in SQL
        self.export_point_datetime_str = export_point_datetime.strftime('%Y-%m-%d %H:%M:%S')
        self.close_timestamp = int(export_point_datetime.timestamp() * 1000)

        # Format delete_older_than for use in SQL
        self.delete_point_datetime_str = delete_point_datetime.strftime('%Y-%m-%d %H:%M:%S')
        self.delete_point_datetime_timestamp = int(delete_point_datetime.timestamp() * 1000)

        self._init_dirs()
    
    def _init_dirs(self):
        # Create directory for given date
        archive_dir = f'{self.dir_name}'
        self.archive_dir_path = conf.ARCHIVE_DIR / archive_dir
        self.archive_dir_path.mkdir(exist_ok=True)
        os.chmod(self.archive_dir_path, 0o777)

        # Init gz dir
        gz_dir = f'{self.dir_name}.tar.gz'
        self.gz_dir_path = conf.ARCHIVE_DIR / gz_dir
        
    def run(self, gzip=True, s3=True, del_pg=True, del_local_dir=True, del_local_gz=True):
        self._export_blocks()
        self._export_transactions()
        self._export_created_transaction_outputs()
        self._export_spent_trasaction_outputs()
        self._export_transaction_inputs()

        # Delete from postgres DB based on param
        # Sequence of these is important
        if del_pg:
            self._delete_blocks()
            self._delete_spent_transaction_outputs()
            self._delete_transaction_inputs()
            self._delete_transactions()
            # not deleting unspent tx outputs

            # with session_maker() as s:
            #     s.execute(text("vacuum analyze"))

        # Gzip based on param
        if gzip:
            self._gz()

        # Put to s3 based on param
        if s3:
            # self._gz()
            # put to s3
            pass

        # Delete local files based on param
        if del_local_dir:
            shutil.rmtree(self.archive_dir_path)

        # Delete gz based on param
        if del_local_gz:
            os.remove(self.gz_dir_path)

    def _gz(self):
        self.gz_dir_path.touch(exist_ok = True)
        with tarfile.open(self.gz_dir_path, "w:gz") as tar:
            tar.add(self.archive_dir_path, arcname=os.path.basename(self.archive_dir_path))

    def _export_blocks(self):
        # Export to CSV
        sql = f"""
            COPY (
                SELECT * FROM blocks WHERE timestamp < '{self.export_point_datetime_str}'
            ) 
            TO '{self.archive_dir_path}/blocks.csv' 
            WITH CSV HEADER;
        """
        
        with session_maker() as s:
            s.execute(text(sql))

    def _delete_blocks(self):
        sql = f"DELETE FROM blocks WHERE timestamp < '{self.delete_point_datetime_str}'"
        
        with session_maker() as s:
            s.execute(text(sql))
            s.commit()

    def _export_transactions(self):
        sql = f"""
        COPY (
            SELECT * FROM transactions WHERE block_time < {self.close_timestamp}
        ) 
        TO '{self.archive_dir_path}/transactions.csv' 
        WITH CSV HEADER;
        """
        
        with session_maker() as s:
            s.execute(text(sql))

    def _delete_transactions(self):
        sql = f"DELETE FROM transactions WHERE block_time < {self.delete_point_datetime_timestamp}"
        
        with session_maker() as s:
            s.execute(text(sql))
            s.commit()
    
    def _export_transaction_inputs(self):
        # Export inputs
        sql = f"""
        COPY (
            SELECT 
            tx_in.transaction_id,
            tx_in.index,
            tx_in.previous_outpoint_hash,
            tx_in.previous_outpoint_index,
            tx_in.signature_script,
            tx_in.sig_op_count
            FROM transactions tx
            JOIN transactions_inputs tx_in
            on tx.transaction_id = tx_in.transaction_id
            WHERE tx.block_time < {self.close_timestamp}
        )
        TO '{self.archive_dir_path}/transaction-inputs.csv' 
        WITH CSV HEADER;
        """
        
        with session_maker() as s:
            s.execute(text(sql))

    def _delete_transaction_inputs(self):
        sql = f"""DELETE FROM transactions_inputs tx_in
        USING transactions tx
        WHERE tx.transaction_id = tx_in.transaction_id
        AND tx.block_time < {self.delete_point_datetime_timestamp}
        """
        
        with session_maker() as s:
            s.execute(text(sql))
            s.commit()

    def _export_spent_trasaction_outputs(self):
        sql = f"""
        COPY (
            select 
            tx_out.transaction_id,
            tx_out.index,
            tx_out.amount,
            tx_out.script_public_key,
            tx_out.script_public_key_address,
            tx_out.script_public_key_type,
            tx_out.accepting_block_hash

            from transactions tx

            join transactions_inputs tx_in
            on tx.transaction_id = tx_in.transaction_id

            left join transactions_outputs tx_out
            on tx_in.previous_outpoint_hash = tx_out.transaction_id 
            and tx_in.previous_outpoint_index = tx_out.index

            where tx.block_time < {self.close_timestamp}
        )
        TO '{self.archive_dir_path}/transaction-outputs-spent.csv' 
        WITH CSV HEADER;
        """
        
        with session_maker() as s:
            s.execute(text(sql))

    def _delete_spent_transaction_outputs(self):
        sql = f"""DELETE FROM transactions_outputs
            USING transactions_inputs, transactions
            WHERE transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash
            AND transactions_outputs.index = transactions_inputs.previous_outpoint_index
            AND transactions_inputs.transaction_id = transactions.transaction_id
            AND transactions.block_time < {self.close_timestamp}
        """
        
        with session_maker() as s:
            s.execute(text(sql))
            s.commit()

    def _export_created_transaction_outputs(self):
        sql = f"""
        COPY (
            SELECT 
            tx_out.transaction_id,
            tx_out.index,
            tx_out.amount,
            tx_out.script_public_key,
            tx_out.script_public_key_address,
            tx_out.script_public_key_type,
            tx_out.accepting_block_hash
            FROM transactions tx
            JOIN transactions_outputs tx_out 
            on tx.transaction_id = tx_out.transaction_id
            WHERE tx.block_time < {self.close_timestamp}
        )
        TO '{self.archive_dir_path}/transaction-outputs-created.csv' 
        WITH CSV HEADER;
        """
        
        with session_maker() as s:
            s.execute(text(sql))

            # Not deleting here since we only want to delete spent tx outputs. This is handled by above

if __name__ == "__main__":
    # Get oldest block date
    with session_maker() as s:
        oldest_block = s.query(Block).order_by(asc(Block.timestamp)).first() 
        oldest_tx = s.query(Transaction).order_by(asc(Transaction.block_time)).first()

    today = datetime.now().date()

    # Always assume target date day after oldest block timestamp in DB (naive)
    target_date = oldest_block.timestamp.date() + timedelta(days=1)

    # Iterate days between target_date and present
    while target_date < today:
        # Convert target_date to datetime
        target_datetime = datetime(
            year=target_date.year, 
            month=target_date.month, 
            day=target_date.day
        )

        # Archive will include data from {23:50:00 day before target_date} to {00:10:00 day after target_date}
        padding_mins = 10
        export_point_datetime = target_datetime + timedelta(days=1, minutes=padding_mins)
        
        # BUT only delete up to 23:50:00 of target date. So that when we archive next day, we can honor the 10 min padding 
        delete_point_datetime = target_datetime + timedelta(hours=23, minutes=50)

        print(f'oldest_block:           {oldest_block.timestamp}')
        print(f'oldest_tx:              {datetime.fromtimestamp(oldest_tx.block_time / 1000)}')
        print(f'target_date:            {target_date}')
        print(f'export_point_datetime:  {export_point_datetime}')
        print(f'delete_point_datetime:  {delete_point_datetime}\n')

        # Init and run archiver for the given time range
        archiver = Archiver(
            dir_name=f'{target_date}_archive',
            export_point_datetime=export_point_datetime,
            delete_point_datetime=delete_point_datetime
        )
        archiver.run(
            del_pg=True,
            del_local_dir=False, 
            del_local_gz=False
        )

        target_date += timedelta(days=1)