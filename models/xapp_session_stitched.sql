-- Cross-app stitched sessions (flagship model)
CREATE TABLE IF NOT EXISTS analytics.xapp_session_stitched (
    global_session_id   STRING,
    user_token          STRING,
    session_start       TIMESTAMP,
    session_end         TIMESTAMP,
    total_events        BIGINT,
    apps_visited        ARRAY<STRING>,
    app_count           INT,
    is_cross_app        BOOLEAN,
    duration_seconds    BIGINT,
    engagement_score    DOUBLE,
    region              STRING,
    platform            STRING,
    stitched_at         TIMESTAMP
)
PARTITIONED BY (dt STRING, platform_combo STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
