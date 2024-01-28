import asyncio
import logging

from sqlalchemy import asc
from sqlalchemy.future import select

from conf.conf import conf
from dbsession import async_session_maker, session_maker
from kaspad.KaspadMultiClient import KaspadMultiClient
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput

_logger = logging.getLogger(__name__)

async def get_dag_info(client):
    r = await client.request(
        "getBlockDagInfoRequest"
    )
    return r['getBlockDagInfoResponse']

async def get_vspc(client, low_hash):
    r = await client.request(
        "getVirtualChainFromBlockRequest",
        { "startHash": low_hash, "includeAcceptedTransactionIds": True },
        timeout=240
    )
    return r['getVirtualChainFromBlockResponse']

class BlocksValidator:
    def __init__(self, block_hashes):
        self.block_hashes = block_hashes

    async def _get_db_blocks(self):
        _logger.info('Querying DB for chunk of Blocks')
        async with async_session_maker() as s:
            sql = select(Block).filter(Block.hash.in_(self.chunk))
            q = await s.execute(sql)
            db_blocks = q.scalars().all()
        
        self.chunk.clear()
        return db_blocks
        
    def _validate_block(self, db_block):
        if db_block.is_chain_block != True:
            _logger.warn(f'!!!!! Block.is_chain_block is {db_block.is_chain_block} for {db_block.hash}. Expecting True !!!!!')

    async def _validate_chunk(self):
        db_blocks = await self._get_db_blocks()

        for db_block in db_blocks:
            self._validate_block(db_block)

    async def validate(self):
        self.chunk = []
        for bh in self.block_hashes:
            self.chunk.append(bh)

            if len(self.chunk) > 1000:
                await self._validate_chunk()

        if self.chunk:
            await self._validate_chunk()
        
        # TODO validate non-chain blocks are properly set to null/False?
        # TODO other block data


class TransactionsValidator:
    def __init__(self, transaction_ids):
        self.transaction_ids = transaction_ids

    async def _get_db_txs(self):
        _logger.info('Querying DB for chunk of Transactions')
        async with async_session_maker() as s:
            sql = select(Transaction).filter(Transaction.transaction_id.in_(self.chunk))
            q = await s.execute(sql)
            db_transactions = q.scalars().all()
        
        self.chunk.clear()
        return db_transactions

    def _validate_tx(self, db_tx):
        if db_tx.is_accepted != True:
            _logger.warn(f'!!!!! Transaction.is_accepted is {db_tx.is_accepted} for {db_tx.transaction_id}. Expecting True !!!!!')

    async def _validate_chunk(self):
        db_txs = await self._get_db_txs()

        for db_tx in db_txs:
            self._validate_tx(db_tx)

    async def validate(self):
        # Transactions
        self.chunk = []
        for tx_id in self.transaction_ids:
            self.chunk.append(tx_id)

            if len(self.chunk) > 1000:
                await self._validate_chunk()
        
        if self.chunk:
            await self._validate_chunk()
        
        # TODO Transaction Inputs
        # TODO Transaction Outputs
        # TODO validate rejected txs are properly set to null/False?
        # TODO other transaction data (accepted block, etc.)


async def async_main():
    """
        Validates database is in sync with node.
    """

    # Get oldest block in DB
    with session_maker() as s:
        low_block = s.query(Block).order_by(asc(Block.timestamp)).first()
    
    _logger.info('low_block = {low_block.hash} {low_block.timestamp}')

    # Init gRPC client
    client = KaspadMultiClient([conf.KASPAD_HOST_1])
    await client.initialize_all()

    # Try to get VSPC from low_block
    vspc = await get_vspc(client, low_block.hash)
    if 'error' in vspc and vspc['error']['message'].startswith('cannot find header'):
        # low_block has been pruned from node. Get pruningPoint and get VSPC from pruningPoint
        dag_info = await get_dag_info(client)
        vspc = await get_vspc(client, dag_info['pruningPointHash']) 

    # Validate all removed blocks are set to is_chain_block = False
    for block in vspc.get('removedChainBlockHashes', []):
        continue

    # Validate all added blocks are set to is_chain_block = True
    bv = BlocksValidator(vspc['addedChainBlockHashes'])
    await bv.validate()

    # Validate transactions
    txs = []
    for d in vspc['acceptedTransactionIds']:
        accepting_block = d['acceptingBlockHash']
        txs.extend(d['acceptedTransactionIds'])
    
    tv = TransactionsValidator(txs)
    await tv.validate()

    # Validate all transactions in VSPC are in DB and marked as accepted = True
    # For all TX in DB not in VSPC, should be marked as false

def main():
    asyncio.run(async_main())