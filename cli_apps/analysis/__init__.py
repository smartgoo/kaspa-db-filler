import typer

from .validate import main as validate_main

# App for analysis related group of commands
app = typer.Typer()

app.command(name="validate")(validate_main)