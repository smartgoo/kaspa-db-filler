-- create temp table
CREATE TEMP TABLE blocks_temp AS SELECT * FROM blocks WITH NO DATA;

-- load into temp table
COPY blocks_temp(
	hash,
	accepted_id_merkle_root,
	difficulty,
	is_chain_block,
	merge_set_blues_hashes,
	merge_set_reds_hashes,
	selected_parent_hash,
	bits,
	blue_score,
	blue_work,
	daa_score,
	hash_merkle_root,
	nonce,
	parents,
	pruning_point,
	timestamp,
	utxo_commitment,
	version
)
FROM '/opt2/kaspa-db-filler/out/2024-01-17_archive/blocks.csv'
DELIMITER ','
CSV HEADER;

-- insert from temporary table with conflict handling
INSERT INTO blocks
SELECT * FROM blocks_temp
ON CONFLICT (hash) DO NOTHING;

-- drop temp table
DROP TABLE blocks_temp; 