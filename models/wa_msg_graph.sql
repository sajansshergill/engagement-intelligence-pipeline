-- WhatsApp messaging graph edges
CREATE TABLE IF NOT EXISTS analytics.wa_msg_graph (
    user_token           STRING,
    target_user_token    STRING,
    msg_type             STRING,
    interaction_count    BIGINT,
    edge_weight          DOUBLE,
    last_msg_at          TIMESTAMP
)
PARTITIONED BY (app STRING, dt STRING, msg_type STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
