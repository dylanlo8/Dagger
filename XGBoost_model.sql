-- These code are Standard SQL code to create a XGBoost model in BigQuery, 
-- utilizing the data from the `datamarts.transactions_additional_info` table, 
-- generated from the ETL process.


-- Step 1: Create Model.
-- This is the finalised model we arrived at. The model takes 1hr 57m to execute.

CREATE OR REPLACE MODEL `hdbprices.xgboost_model14`
OPTIONS (
    model_type='BOOSTED_TREE_REGRESSOR',
    input_label_cols=['resale_price'],
    max_iterations= 100,
    BOOSTER_TYPE = HPARAM_CANDIDATES(['GBTREE', 'DART']),
    DART_NORMALIZE_TYPE = HPARAM_CANDIDATES(['TREE', 'FOREST']),
    learn_rate = HPARAM_RANGE(0.1, 0.5),
    DROPOUT = HPARAM_RANGE(0, 0.6),
    DATA_SPLIT_METHOD = 'AUTO_SPLIT',
    EARLY_STOP = TRUE,
    ENABLE_GLOBAL_EXPLAIN = TRUE,
    HPARAM_TUNING_ALGORITHM = 'VIZIER_DEFAULT',
    NUM_TRIALS = 60,
    L2_REG = HPARAM_RANGE(0.5, 5.0),
    XGBOOST_VERSION = '1.1',
    MAX_PARALLEL_TRIALS = 5,
    HPARAM_TUNING_OBJECTIVES = ['MEAN_ABSOLUTE_ERROR']
) AS
SELECT
    month,
    town,
    flat_type,
    block,
    street_name,
    storey_range,
    flat_model,
    lease_commence_year,
    floor_area_sqm,
    postal_code,
    max_floor_lvl,
    year_completed,
    multistorey_carpark,
    precinct_pavilion,
    market_hawker,
    resale_price
FROM (
    SELECT
        PARSE_DATE('%Y-%m', month) AS month,
        town,
        flat_type,
        block,
        street_name,
        storey_range,
        flat_model,
        lease_commence_year,
        floor_area_sqm,
        postal_code,
        max_floor_lvl,
        year_completed,
        multistorey_carpark,
        precinct_pavilion,
        market_hawker,
        resale_price
    FROM
        `datamarts.transactions_additional_info`
    ORDER BY month
);

-- Step 2: Evaluate Model.
-- This code evaluates the model we created in Step 1, returning the evaluation metrics for all trials.
SELECT
  *
FROM
  ML.EVALUATE(MODEL `hdbprices.xgboost_model14`);

-- Step 3: Predict with model.
-- This code predicts the resale price of HDB flats using the model we created in Step 1. 
-- It can be adapted to predict for different months and years by changing the month in the SELECT statement.
SELECT
 *
FROM
  ML.PREDICT(MODEL `hdbprices.xgboost_model14`,
    (
      SELECT
        PARSE_DATE('%Y-%m', month) AS month,
        town,
        flat_type,
        block,
        street_name,
        storey_range,
        flat_model,
        lease_commence_year,
        floor_area_sqm,
        postal_code,
        max_floor_lvl,
        year_completed,
        multistorey_carpark,
        precinct_pavilion,
        market_hawker, 
        resale_price
      FROM
        `datamarts.transactions_additional_info`
    )
  );