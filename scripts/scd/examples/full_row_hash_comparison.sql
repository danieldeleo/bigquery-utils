-- Create a temp table where one row is changed.
CREATE TEMP TABLE json_row_hashes AS
SELECT
  SHA256(TO_JSON_STRING(t)) row_hash,
FROM(
  SELECT
    block_id,
    IF(block_id = "0000000000000ba1df5fc77872b1225830ccb94d891e0b8643dc8bf140b7e018", "0", previous_block) AS previous_block,
    * EXCEPT(block_id, previous_block),
  FROM 
    `bigquery-public-data.bitcoin_blockchain.blocks`
) t;

-- Less performant way of finding changed rows
SELECT 
  row_hash
FROM 
  json_row_hashes
WHERE row_hash NOT IN(
  SELECT DISTINCT SHA256(TO_JSON_STRING(t)) row_hash
  FROM `bigquery-public-data.bitcoin_blockchain.blocks` t
);

-- More performant way of finding changed rows
SELECT
  row_hash
FROM 
  json_row_hashes
EXCEPT DISTINCT(
  SELECT SHA256(TO_JSON_STRING(t)) row_hash
  FROM `bigquery-public-data.bitcoin_blockchain.blocks` t
);