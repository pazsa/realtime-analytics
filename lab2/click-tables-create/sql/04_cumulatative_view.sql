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
