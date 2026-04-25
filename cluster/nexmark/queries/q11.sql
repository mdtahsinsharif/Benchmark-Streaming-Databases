-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q11 (
    bidder BIGINT,
    bid_count BIGINT,
    starttime TIMESTAMP(3),
    endtime TIMESTAMP(3),
    event_time TIMESTAMP(3),       -- timestamp of the first bid in the session
    processed_time TIMESTAMP(3)    -- timestamp after processing
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///srv/nfs/flink/nexmark/failure/cpu/q11/p10',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.disable-quote-character' = 'true'
);

INSERT INTO nexmark_q11
SELECT
    B.bidder,
    COUNT(*) AS bid_count,
    SESSION_START(B.`dateTime`, INTERVAL 10 SECOND) AS starttime,
    SESSION_END(B.`dateTime`, INTERVAL 10 SECOND) AS endtime,
    MIN(B.`dateTime`) AS event_time,        -- first bid in the session as event time
    CURRENT_TIMESTAMP AS processed_time   -- processing timestamp
FROM bid B
GROUP BY B.bidder, SESSION(B.`dateTime`, INTERVAL 10 SECOND);