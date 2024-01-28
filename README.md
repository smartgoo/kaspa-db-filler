# Overview
Primary app is `main.py` that runs `kaspa-db-filler` app.

Collection of misc. database scripts/utilities exist in `cli.py`.

# Apps
## main.py
`pipenv run python main.py`

## cli.py
A [typer](https://typer.tiangolo.com) CLI app for various database utilities. Currently provides functionality to archive data. Restoring data is WIP. 

To run: `pipenv run python cli.py --help`

# Misc Notes
Proto specs in `./kaspad/protos-rust` are from [rusty-kaspa v0.13.3](https://github.com/kaspanet/rusty-kaspa/releases/tag/v0.13.3)