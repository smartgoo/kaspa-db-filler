-- create temp table
CREATE TEMP TABLE transactions_inputs_temp AS SELECT * FROM transactions_inputs WITH NO DATA;

-- load into temp table
COPY transactions_inputs_temp(
	transaction_id,
	index,
	previous_outpoint_hash,
	previous_outpoint_index,
	signature_script,
	sig_op_count
)
FROM '/opt2/kaspa-db-filler/out/2024-01-18_archive/transaction-inputs.csv'
DELIMITER ','
CSV HEADER;

-- Insert from Temporary Table with Conflict Handling
INSERT INTO transactions_inputs (
	transaction_id,
	index,
	previous_outpoint_hash,
	previous_outpoint_index,
	signature_script,
	sig_op_count
)
SELECT 
transaction_id,
index,
previous_outpoint_hash,
previous_outpoint_index,
signature_script,
sig_op_count
FROM transactions_inputs_temp

-- Drop temp table
DROP TABLE transactions_inputs_temp; 