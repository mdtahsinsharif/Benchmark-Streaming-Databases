-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

--CREATE TABLE nexmark_q1 (
--  auction  BIGINT,
--  bidder  BIGINT,
--  price  DECIMAL(23, 3),
--  `dateTime`  TIMESTAMP(3),
--  extra  VARCHAR
--) WITH (
--  --'connector' = 'blackhole'
--  'connector' = 'filesystem',
--  'path' = 'file:///tmp/nexmark_results',
--  'format' = 'csv'
--);

--INSERT INTO nexmark_q1
--SELECT
--    auction,
--    bidder,
--    0.908 * price as price, -- convert dollar to euro
--    `dateTime`,
--    extra
--FROM bid;

-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

-- -------------------------------------------------------------------------------------------------
-- Query1 with latency instrumentation (start + end timestamps)
-- -------------------------------------------------------------------------------------------------
/*CREATE TABLE nexmark_q1 (
    auction          BIGINT,
    bidder           BIGINT,
    price            DECIMAL(23, 3),
    dateTime         TIMESTAMP(3),     -- event time
    start_time       TIMESTAMP(3),     -- before computation
    end_time         TIMESTAMP(3),     -- after computation
    compute_ms       BIGINT,           -- duration in ms
    extra            STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/nexmark_q1',
    'format' = 'csv'
);

-- -------------------------------------------------------
-- Step 1: Capture start time before computation
-- -------------------------------------------------------
CREATE VIEW bid_with_start AS
SELECT
    auction,
    bidder,
    price,
    `dateTime`,
    CURRENT_TIMESTAMP AS start_time,
    extra
FROM bid;

-- -------------------------------------------------------
-- Step 2: Compute, capture end time, and measure latency
-- -------------------------------------------------------
INSERT INTO nexmark_q1
SELECT
    auction,
    bidder,
    0.908 * price AS price,                              -- computation
    dateTime,                                            -- original event time
    start_time,                                          -- captured before computation
    CURRENT_TIMESTAMP AS end_time,                       -- after computation
    FLOOR(TIMESTAMPDIFF(FRAC_SECOND, start_time, CURRENT_TIMESTAMP) * 1000) AS compute_ms,
    extra
FROM bid_with_start;
*/

CREATE TABLE nexmark_q1 (
    auction BIGINT,
    bidder BIGINT,
    price DECIMAL(23,3),
    event_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),
    extra VARCHAR
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///srv/nfs/flink/nexmark/failure/cpu/q1/p10',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.disable-quote-character' = 'true'
);

INSERT INTO nexmark_q1
SELECT
    auction,
    bidder,
    0.908 * price AS price,           -- convert dollar to euro
    `dateTime` AS event_time,         -- original bid timestamp
    CURRENT_TIMESTAMP AS processed_time, -- timestamp after processing
    extra
FROM bid;