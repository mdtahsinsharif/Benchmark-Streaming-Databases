-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q7 (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    event_time TIMESTAMP(3),       -- timestamp of bid/event generation
    processed_time TIMESTAMP(3),   -- timestamp after processing
    extra VARCHAR
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///srv/nfs/flink/nexmark/failure/q7/p10',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.disable-quote-character' = 'true'
);

INSERT INTO nexmark_q7
SELECT 
    B.auction,
    B.bidder,
    B.price,
    B.`dateTime` AS event_time,        -- original bid timestamp
    CURRENT_TIMESTAMP AS processed_time, -- processing timestamp
    B.extra
FROM bid B
JOIN (
    SELECT 
        MAX(price) AS maxprice, 
        window_end AS window_end_time
    FROM TABLE(
        TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND)
    )
    GROUP BY window_start, window_end
) B1
ON B.price = B1.maxprice
WHERE B.`dateTime` BETWEEN B1.window_end_time - INTERVAL '10' SECOND AND B1.window_end_time;