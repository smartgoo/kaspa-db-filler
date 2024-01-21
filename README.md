# Overview
Primary app is `main.py` that runs `kaspa-db-filler` app.

Collection of misc. database scripts/utilities exist in `db_utils_cli.py`.

# Apps
## main.py
`pipenv run python main.py`

## db_utils_cli.py
A [typer](https://typer.tiangolo.com]) CLI app for various database utilities. Currently provides functionality to archive data. Restoring data is WIP. 

To run: `pipenv run python db_utils_cli.py --help`

### `archive` command

#### args
| Arg | Note | Type | Default |
|-----|------|------|---------|
|`del_pg`| Delete data in Postgres database after export? | bool | False |
| `del_csvs` | Delete local csvs after export? | bool | False | 

#### High Level Flow
1. Gets oldest block in database.
2. If the oldest block is greater than 2 days ago, the program assumes `oldest_block.timestamp + timedelta(days=1)` is the `target_date`. E.g. `oldest_block.timestamp = 1/1/2024`, then `target_date = 1/2/2024`.
3. Uses PG COPY to export all data for `target_date` to CSVs in  `./out/<target_date>_archive/`:
- `blocks.csv`
- `transactions.csv`
- `transaction-inputs.csv`
- `transaction-outputs.csv`  - contains all created outputs and spent outputs
4. Creates a tarball of `./out/<target_date>_archive`

# Misc Notes
Proto specs in `./kaspad/protos-rust` are from [rusty-kaspa v0.13.3](https://github.com/kaspanet/rusty-kaspa/releases/tag/v0.13.3)