-- Instagram story / reel engagement signals
CREATE TABLE IF NOT EXISTS analytics.ig_story_interactions (
    user_token      STRING,
    content_id      STRING,
    event_type      STRING,
    session_id      STRING,
    region          STRING,
    platform        STRING,
    event_ts        TIMESTAMP
)
PARTITIONED BY (app STRING, dt STRING, content_type STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
