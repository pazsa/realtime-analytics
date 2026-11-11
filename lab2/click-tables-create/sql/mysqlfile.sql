CREATE TABLE lab02.kafka_ingest(
	dataset_id   String,
	event_id     String,
	order_id     String,
	campaign_id  String,
	status       String,
	version      UInt32,
	amount_cents Int64,
	is_deleted   UInt8,
	created_at   DateTime64(3, 'UTC'),
	event_time   DateTime64(3, 'UTC')
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092',
                          kafka_topic_list = 'lab02.orders',
                          kafka_group_name = 'lab02_consumer_group_1',
                          kafka_format = 'JSONEachRow',
			  date_time_input_format = 'best_effort';

CREATE TABLE lab02.check_raw(
	dataset_id   String,
	event_id     String,
	order_id     String,
	campaign_id  String,
	status       String,
	version      UInt32,
	amount_cents Int64,
	is_deleted   UInt8,
	created_at   DateTime64(3, 'UTC'),
	event_time   DateTime64(3, 'UTC')
) ENGINE = MergeTree()
  PRIMARY KEY (dataset_id, campaign_id)
  SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW lab02.raw_mv TO lab02.check_raw AS
SELECT *
  FROM lab02.kafka_ingest;

CREATE TABLE lab02.check_current_not_final(
	dataset_id   String,
	event_id     String,
	order_id     String,
	campaign_id  String,
	status       String,
	version      UInt32,
	amount_cents Int64,
	is_deleted   UInt8,
	created_at   DateTime64(3, 'UTC'),
	event_time   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(version)
  ORDER BY (dataset_id, order_id)
  SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW lab02.current_mv TO lab02.check_current_not_final AS
SELECT *
FROM lab02.check_raw;

CREATE OR REPLACE VIEW lab02.check_current AS
SELECT *
FROM lab02.check_current_not_final FINAL;


CREATE VIEW lab02.metrics_paid_amount AS
SELECT
    {p_dataset_id:String} AS dataset_id,
    sum(amount_cents) AS paid_amount_cents
FROM lab02.check_current
WHERE dataset_id = {p_dataset_id:String}
  AND status = 'paid'
  AND is_deleted = 0;
CREATE OR REPLACE VIEW lab02.metrics_paid_hourly AS
WITH hourly AS (
    SELECT dataset_id
         , campaign_id
         , toDateTime64(toStartOfHour(created_at), 3, 'UTC') AS bucket_start
         , sum(amount_cents) AS paid_amount_cents
      FROM lab02.check_current
     WHERE dataset_id = {p_dataset_id:String}
       AND status = 'paid'
       AND is_deleted = 0
     GROUP BY dataset_id, campaign_id, bucket_start
)
SELECT dataset_id
     , campaign_id
     , bucket_start
     , paid_amount_cents
     , sum(paid_amount_cents) OVER (
           PARTITION BY campaign_id
           ORDER BY bucket_start
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS cumulative_paid_amount_cents
  FROM hourly
