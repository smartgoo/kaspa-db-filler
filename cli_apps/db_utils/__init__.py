import typer

from .archive import main as archive_main
from .restore import main as restore_main
from .restored_tables import (
    truncate_all_restored_tables,
    truncate_restored_blocks,
    truncate_restored_transactions,
    truncate_restored_transaction_inputs,
    truncate_restored_transaction_outputs
)

# App for db_util related group of commands
app = typer.Typer()

app.command(name="archive")(archive_main)
app.command(name="restore")(restore_main)

app.command(name="truncate-restored-all")(truncate_all_restored_tables)
app.command(name="truncate-restored-blocks")(truncate_restored_blocks)
app.command(name="truncate-restored-transactions")(truncate_restored_transactions)
app.command(name="truncate-restored-transactions-inputs")(truncate_restored_transaction_inputs)
app.command(name="truncate-restored-outputs")(truncate_restored_transaction_outputs)