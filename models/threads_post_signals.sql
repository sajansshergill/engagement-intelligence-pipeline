-- Threads post engagement
CREATE TABLE IF NOT EXISTS analytics.threads_post_signals (
    user_token      STRING,
    post_id         STRING,
    event_type      STRING,
    session_id      STRING,
    region          STRING,
    event_ts        TIMESTAMP
)
PARTITIONED BY (app STRING, dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
