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
SELECT 'refrigerator' AS unique_key, 11 AS quantity, CAST(NULL AS BOOL) AS supply_constrained, CURRENT_DATE AS eff_dt
UNION ALL 
SELECT 'microwave', 22, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt
UNION ALL 
SELECT 'dryer', 231, CAST(NULL AS BOOL), CURRENT_DATE AS eff_dt;

-- The following example assumes staging data is made up of delta records only.

-- Merge all data back to target
MERGE target t
USING (
  -- Records in staging data which have changed or are new
  -- These records are used for the following clauses:
  --     - "WHEN NOT MATCHED BY TARGET THEN"
  SELECT s.*, CAST(NULL AS DATE) AS expir_dt
  FROM target t
  JOIN staging s USING(
    -- BEGIN dynamic injection
    unique_key
    -- END dynamic injection
  )
  WHERE t.expir_dt IS NULL
  UNION ALL
  -- Latest records from target table which are present in staging data.
  -- These records are used for the "WHEN MATCHED THEN" clause
  SELECT t.* EXCEPT(expir_dt),
  s.eff_dt AS expir_dt
  FROM target t
  JOIN staging s USING(
    -- BEGIN dynamic injection
    unique_key
    -- END dynamic injection
  )
  WHERE t.expir_dt IS NULL
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
