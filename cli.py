import logging

import typer

from conf.log import configure_logger
import db_utils
import analysis

configure_logger()
_logger = logging.getLogger(__name__)

# Primary typer CLI app
app = typer.Typer()

# Register db_utils typer app to primary typer app as a command group
app.add_typer(db_utils.app, name="db-utils")

# Register analysis typer app to primary typer app as a command group
app.add_typer(analysis.app, name="analysis")

if __name__ == "__main__":
    app()