from datetime import datetime
import logging
import tarfile
from typing_extensions import Annotated

from sqlalchemy.sql import text
import typer

from conf.conf import conf
from dbsession import session_maker

_logger = logging.getLogger(__name__)


def main(
    date: Annotated[
        datetime,
        typer.Argument(
            formats=["%Y-%m-%d"]
        )
    ],
):  
    # Restore blocks
    _logger.info("Restoring blocks to restored_blocks table")
    with session_maker() as s:
        sql = f"""
            COPY restored_blocks(
                hash,
                accepted_id_merkle_root,
                difficulty,
                is_chain_block,
                merge_set_blues_hashes,
                merge_set_reds_hashes,
                selected_parent_hash,
                bits,
                blue_score,
                blue_work,
                daa_score,
                hash_merkle_root,
                nonce,
                parents,
                pruning_point,
                timestamp,
                utxo_commitment,
                version
            )
            FROM '{conf.ARCHIVE_DIR}/{date.date()}_archive/blocks.csv'
            DELIMITER ','
            CSV HEADER;
        """
        s.execute(text(sql))
        s.commit()

    # Restore transactions
    _logger.info("Restoring transactions to restored_transactions table")
    with session_maker() as s:
        sql = f"""
            COPY restored_transactions(
                subnetwork_id,
                transaction_id,
                hash,
                mass,
                block_hash,
                block_time,
                is_accepted,
                accepting_block_hash
            )
            FROM '{conf.ARCHIVE_DIR}/{date.date()}_archive/transactions.csv'
            DELIMITER ','
            CSV HEADER;
        """
        s.execute(text(sql))
        s.commit()

    # Restore tx inputs
    _logger.info("Restoring transaction inputs to restored_transactions_inputs table")
    with session_maker() as s:
        sql = f"""
            COPY restored_transactions_inputs(
                transaction_id,
                index,
                previous_outpoint_hash,
                previous_outpoint_index,
                signature_script,
                sig_op_count
            )
            FROM '{conf.ARCHIVE_DIR}/{date.date()}_archive/transaction-inputs.csv'
            DELIMITER ','
            CSV HEADER;
        """
        s.execute(text(sql))
        s.commit()

    # Restore tx outputs
    _logger.info("Restoring transaction outputs to restored_transactions_outputs table")
    with session_maker() as s:
        sql = f"""
            COPY restored_transactions_outputs(
                transaction_id,
                index,
                amount,
                script_public_key,
                script_public_key_address,
                script_public_key_type,
                accepting_block_hash
            )
            FROM '{conf.ARCHIVE_DIR}/{date.date()}_archive/transaction-outputs.csv' -- swap out with *-created.csv as well
            DELIMITER ','
            CSV HEADER;
        """
        s.execute(text(sql))
        s.commit()