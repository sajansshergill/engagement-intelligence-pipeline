-- Facebook engagement graph (Hive ORC). Partition: app, date, region
CREATE TABLE IF NOT EXISTS analytics.fb_engagement_edges (
    user_token           STRING,
    target_user_token    STRING,
    event_type           STRING,
    interaction_count    BIGINT,
    raw_edge_weight      DOUBLE,
    final_edge_weight    DOUBLE,
    normalized_weight    DOUBLE,
    last_interaction_at  TIMESTAMP,
    computed_at          TIMESTAMP
)
PARTITIONED BY (app STRING, dt STRING, region STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
