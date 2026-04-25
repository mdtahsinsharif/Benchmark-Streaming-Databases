-- -------------------------------------------------------------------------------------------------
-- Query 14: Calculation (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Convert bid timestamp into types and find bids with specific price.
-- Illustrates duplicate expressions and usage of user-defined-functions.
-- -------------------------------------------------------------------------------------------------

CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

CREATE TABLE nexmark_q14 (
    auction BIGINT,
    bidder BIGINT,
    price DECIMAL(23, 3),
    bidTimeType VARCHAR,
    event_time TIMESTAMP(3),       -- timestamp of the bid (event generation)
    processed_time TIMESTAMP(3),   -- timestamp after processing
    extra VARCHAR,
    c_counts BIGINT
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///srv/nfs/flink/nexmark/scalability/q14/p30',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.disable-quote-character' = 'true'
);

INSERT INTO nexmark_q14
SELECT 
    auction,
    bidder,
    0.908 * price AS price,
    CASE
        WHEN HOUR(`dateTime`) >= 8 AND HOUR(`dateTime`) <= 18 THEN 'dayTime'
        WHEN HOUR(`dateTime`) <= 6 OR HOUR(`dateTime`) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    `dateTime` AS event_time,        -- original bid timestamp
    CURRENT_TIMESTAMP AS processed_time,
    extra,
    count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;