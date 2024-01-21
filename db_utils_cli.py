import logging

import typer

from conf.log import configure_logger
from db_utils.archive import main as archive_main

configure_logger()
_logger = logging.getLogger(__name__)

app = typer.Typer()

@app.command()
def archive(
    del_pg: bool = False, 
    del_csvs: bool = False,
):
    archive_main(
        del_pg, 
        del_csvs,
        False # Never delete tarball...
    )

@app.command()
def restore():
    pass

if __name__ == "__main__":
    app()