-- create temp table
CREATE TEMP TABLE transactions_outputs_temp AS SELECT * FROM transactions_outputs WITH NO DATA;

-- load into temp table
COPY transactions_outputs_temp(
	transaction_id,
	index,
	amount,
	script_public_key,
	script_public_key_address,
	script_public_key_type,
	accepting_block_hash
)
FROM '/opt2/kaspa-db-filler/out/2024-01-17_archive/transaction-outputs-spent.csv' -- swap out with *-created.csv as well
DELIMITER ','
CSV HEADER;

-- Insert from Temporary Table with Conflict Handling
INSERT INTO transactions_outputs (
	transaction_id,
	index,
	amount,
	script_public_key,
	script_public_key_address,
	script_public_key_type,
	accepting_block_hash
)
SELECT 
transaction_id,
index,
amount,
script_public_key,
script_public_key_address,
script_public_key_type,
accepting_block_hash
FROM transactions_outputs_temp

-- Drop temp table
DROP TABLE transactions_outputs_temp;