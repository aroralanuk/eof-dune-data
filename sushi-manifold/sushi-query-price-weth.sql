WITH weth_pairs AS ( -- Get exchange contract address and "other token" for WETH
    SELECT cr."pair" AS contract,
        CASE WHEN cr."token0" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then '0' ELSE '1' END  AS eth_token,
        CASE WHEN cr."token1" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then cr."token0" ELSE cr."token1" END  AS other_token
    FROM sushi."Factory_evt_PairCreated" cr
    WHERE token0 = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' OR  token1 = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    )

, swap AS ( -- Get all trades on the pair last 14 days
    SELECT
        CASE WHEN eth_token = '0' then sw."amount0In" + sw."amount0Out" ELSE sw."amount1In" + sw."amount1Out"
        END/1e18 AS eth_amt,
        CASE WHEN eth_token = '1' then sw."amount0In" + sw."amount0Out" ELSE sw."amount1In" + sw."amount1Out"
        END/power(10, tok."decimals") AS other_amt, -- If the token is not in the erc20.tokens list you can manually divide by 10^decimals
        tok."symbol",
        tok."contract_address",
        date_trunc("minute", sw."evt_block_time") AS time_stamp
    FROM sushi."Pair_evt_Swap" sw
    JOIN weth_pairs ON sw."contract_address" = weth_pairs."contract"
    JOIN erc20."tokens" tok ON weth_pairs."other_token" = tok."contract_address"
    WHERE other_token = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' --renBTC example
    -- To allow users to submit token address in the Dune UI you can use the below line:
    -- WHERE other_token = CONCAT('\x', substring('{{Token address}}' from 3))::bytea -- Allow user to input 0x... format and convert to \x... format
    AND sw.evt_block_time >= now() - interval '14 days'
    )

, eth_prcs AS (
    SELECT avg(price) eth_prc, minute AS time_stamp
    FROM prices.layer1_usd_eth
    WHERE minute >= now() - interval '14 days'
    group by 2 )

SELECT
    AVG((eth_amt/other_amt)*eth_prc) AS usd_price,
    swap."symbol" AS symbol,
    swap."contract_address" AS contract_address,
    eth_prcs."hour" AS hour
FROM swap JOIN eth_prcs ON swap."time_stamp" = eth_prcs."time_stamp"
GROUP BY 2,3,4



