WITH pool_contract AS (SELECT ('\' || RIGHT('\x397ff1542f962076d0bfe58ea045ffa2d347aca0', -1))::bytea AS pc),
    aggregation AS (SELECT 'day'AS a),

 start_and_end_date AS (
    SELECT CASE WHEN '2020-01-01' < NOW() THEN '2020-01-01'::timestamp
        ELSE '2000-01-01'::timestamp END AS sd,
    CASE WHEN '2024-01-01' < NOW() THEN '2024-01-01'::timestamp
        ELSE NOW() END AS ed
    )
, sushiswap AS (
    SELECT pair AS contract_address
    , 'SushiSwap' AS project
    , 0.3 AS lp_fee_percentage
    , evt_block_time AS block_time
    FROM sushi."Factory_evt_PairCreated"
    )
,
 all_pools AS (SELECT * FROM sushiswap
    WHERE contract_address =  (SELECT pc FROM pool_contract)
    )

, dex_pool_fees AS (SELECT DISTINCT ON (start_lp.contract_address, start_lp.block_time) start_lp.contract_address AS contract_address
    , start_lp.project
    , start_lp.lp_fee_percentage
    , start_lp.block_time AS start_block_time
    , COALESCE(end_lp.block_time, '3000-01-01') AS end_block_time
    FROM (
        SELECT * FROM all_pools ORDER BY "contract_address", block_time
        ) start_lp
    LEFT JOIN (
        SELECT * FROM all_pools ORDER BY "contract_address", block_time
        ) end_lp
    ON start_lp.contract_address = end_lp.contract_address AND start_lp.block_time < end_lp.block_time
    )

, time_series AS (
    SELECT distinct date_trunc((SELECT a FROM aggregation) ,generate_series(date_trunc('day', minimum_date),  maximum_date, '1 day')) AS time
    FROM (
        SELECT CASE WHEN sd < (SELECT MIN(start_block_time) FROM dex_pool_fees) THEN (SELECT MIN(start_block_time) FROM dex_pool_fees)
        ELSE sd END AS minimum_date
        , ed AS maximum_date
        FROM start_and_end_date
    ) md
    )

, pooled_tokens AS (
    SELECT CASE WHEN token_a_symbol > token_b_symbol THEN token_b_address ELSE token_a_address END AS token_a
    , CASE WHEN token_a_symbol > token_b_symbol THEN token_a_address ELSE token_b_address END AS token_b
    , CASE WHEN token_a_symbol > token_b_symbol THEN token_b_symbol ELSE token_a_symbol END AS token_a_symbol
    , CASE WHEN token_a_symbol > token_b_symbol THEN token_a_symbol ELSE token_b_symbol END AS token_b_symbol
    , CASE WHEN token_a_symbol > token_b_symbol THEN POWER(10, erctb.decimals) ELSE POWER(10, ercta.decimals) END AS token_a_decimals
    , CASE WHEN token_a_symbol > token_b_symbol THEN POWER(10, ercta.decimals) ELSE POWER(10, erctb.decimals) END AS token_b_decimals
    FROM dex.trades dt
    LEFT JOIN erc20."tokens" ercta ON dt.token_a_address=ercta.contract_address
    LEFT JOIN erc20."tokens" erctb ON dt.token_b_address=erctb.contract_address
    WHERE exchange_contract_address=(SELECT pc FROM pool_contract)
    LIMIT 1
    )


SELECT time AS "Date"
, MIN(price_token_a) AS "Token A Price"
, MIN(price_token_b) AS "Token B Price"
FROM
    (SELECT ts.time
    , price AS price_token_a
    , NULL AS price_token_b
    FROM time_series ts
    LEFT JOIN prices.usd pu ON ts.time=pu.minute
    WHERE contract_address=(SELECT token_a FROM pooled_tokens)
    UNION
    SELECT ts.time
    , NULL AS price_token_a
    , price AS price_token_b
    FROM time_series ts
    LEFT JOIN prices.usd pu ON ts.time=pu.minute
    WHERE contract_address=(SELECT token_b FROM pooled_tokens)
    ) ungrouped
GROUP BY "Date"
