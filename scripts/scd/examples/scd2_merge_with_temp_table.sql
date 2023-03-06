CREATE TEMP TABLE target AS
SELECT 'refrigerator' AS unique_key, 10 AS quantity, CAST(NULL AS BOOL) AS supply_constrained, CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE) AS expir_dt
UNION ALL 
SELECT 'microwave', 20, CAST(NULL AS BOOL), CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'dryer', 230, CAST(NULL AS BOOL), CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'oven', 305, CAST(NULL AS BOOL), CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'top load washer', CAST(NULL AS INTEGER), CAST(NULL AS BOOL), CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE),
UNION ALL 
SELECT 'front load washer', 20, CAST(NULL AS BOOL), CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'dishwasher', 30, CAST(NULL AS BOOL), CURRENT_DATE-1 AS eff_dt, CAST(NULL AS DATE);

CREATE TEMP TABLE staging AS
SELECT 'refrigerator' AS unique_key, 11 AS quantity, CAST(NULL AS BOOL) AS supply_constrained, CURRENT_DATE AS eff_dt, CAST(NULL AS DATE) AS expir_dt
UNION ALL 
SELECT 'microwave', 20, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'dryer', 230, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'oven', 305, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'top load washer', 110, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt, CAST(NULL AS DATE)
-- Commenting below simulates a deleted record
-- UNION ALL 
-- SELECT 'front load washer', 20, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt, CAST(NULL AS DATE)
UNION ALL 
SELECT 'dishwasher', 30, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt, CAST(NULL AS DATE);

-- Gets the latest records from target
-- which are present in staging.
-- This excludes any deleted records.
-- This latest_records temp table does not
-- need to be created; the inner SQL can
-- instead be used as a subquery in the MERGE
-- statement in the two areas where latest_records
-- is used.
CREATE TEMP TABLE latest_records
CLUSTER BY unique_key, eff_dt AS
SELECT
-- BEGIN dynamic injection (all staging columns except expir_dt)
t.unique_key, t.quantity, t.supply_constrained, t.eff_dt
-- END dynamic injection
,IF(1=1
  -- BEGIN dynamic injection (all staging columns except eff_dt, expir_dt)
  AND (IFNULL(t.quantity = s.quantity, FALSE) OR (t.quantity IS NULL AND s.quantity IS NULL)) 
  AND (IFNULL(t.supply_constrained = s.supply_constrained, FALSE) OR (t.supply_constrained IS NULL AND s.supply_constrained IS NULL))
  -- END dynamic injection
  ,t.expir_dt -- Don't expire because nothing's changed
  ,s.eff_dt -- Expire any records which have changed
) AS expir_dt
FROM target t
JOIN staging s USING(
  -- BEGIN dynamic injection
  unique_key
  -- END dynamic injection
)
WHERE t.expir_dt IS NULL;

-- Merge all data back to target
MERGE target t
USING (
  SELECT s.* 
  FROM staging s
  LEFT JOIN latest_records l USING(
    -- BEGIN dynamic injection
    unique_key
    -- END dynamic injection
  )
  WHERE NOT(1=1
    -- BEGIN dynamic injection (all staging columns except eff_dt, expir_dt)
    AND (IFNULL(l.quantity = s.quantity, FALSE) OR (l.quantity IS NULL AND s.quantity IS NULL)) 
    AND (IFNULL(l.supply_constrained = s.supply_constrained, FALSE) OR (l.supply_constrained IS NULL AND s.supply_constrained IS NULL))
    -- END dynamic injection
  )
  UNION ALL 
  SELECT
    -- BEGIN dynamic injection (all staging columns)
    unique_key, quantity, supply_constrained, eff_dt, expir_dt
    -- END dynamic injection
  FROM latest_records
) s
ON t.eff_dt = s.eff_dt 
-- BEGIN dynamic injection
AND t.unique_key = s.unique_key
-- END dynamic injection

WHEN MATCHED THEN
  -- Expire previous version of record
  UPDATE SET t.expir_dt = s.expir_dt
WHEN NOT MATCHED BY TARGET THEN
  -- Insert new records
  INSERT(
    -- BEGIN dynamic injection
    unique_key, quantity, supply_constrained, eff_dt
    -- END dynamic injection
  )
  VALUES(
    -- BEGIN dynamic injection
    unique_key, quantity, supply_constrained, eff_dt
    -- END dynamic injection
  )
WHEN NOT MATCHED BY SOURCE THEN
  -- Set expiry for deleted records
  -- or remove this last step if not needed
  UPDATE SET expir_dt = CURRENT_DATE();