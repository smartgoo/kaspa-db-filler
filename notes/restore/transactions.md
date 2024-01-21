-- create temp table
CREATE TEMP TABLE transactions_temp AS SELECT * FROM transactions WITH NO DATA;

-- load into temp table
COPY transactions_temp(
	subnetwork_id,
	transaction_id,
	hash,
	mass,
	block_hash,
	block_time,
	is_accepted,
	accepting_block_hash
)
FROM '/opt2/kaspa-db-filler/out/2024-01-18_archive/transactions.csv'
DELIMITER ','
CSV HEADER;

-- Insert from Temporary Table with Conflict Handling
INSERT INTO transactions
SELECT * FROM transactions_temp
ON CONFLICT (transaction_id) DO NOTHING;

-- Drop temp table
DROP TABLE transactions_temp; 