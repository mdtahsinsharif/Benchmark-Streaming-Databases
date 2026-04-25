-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
--
-- In original Nexmark queries, Query2 is as following (in CQL syntax):
--
--   SELECT Rstream(auction, price)
--   FROM Bid [NOW]
--   WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
--
-- However, that query will only yield a few hundred results over event streams of arbitrary size.
-- To make it more interesting we instead choose bids for every 123'th auction.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q2 (
    auction BIGINT,
    price BIGINT,
    event_time TIMESTAMP(3),       -- timestamp of event generation
    processed_time TIMESTAMP(3)    -- timestamp after processing
) WITH (
    'connector' = 'filesystem',    -- store persistently
    'path' = 'file:///srv/nfs/flink/nexmark/failure/q2/p10',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.disable-quote-character' = 'true'
);

INSERT INTO nexmark_q2
SELECT
    auction,
    price,
    `dateTime` AS event_time,      -- original bid timestamp
    CURRENT_TIMESTAMP AS processed_time  -- processing timestamp
FROM bid
WHERE MOD(auction, 123) = 0;