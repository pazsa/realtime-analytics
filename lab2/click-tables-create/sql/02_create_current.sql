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


