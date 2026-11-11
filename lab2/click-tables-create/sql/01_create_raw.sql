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

