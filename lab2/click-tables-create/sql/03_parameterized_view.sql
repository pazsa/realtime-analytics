CREATE OR REPLACE VIEW lab02.metrics_paid_amount AS
SELECT
    {p_dataset_id:String} AS dataset_id,
    sum(amount_cents) AS paid_amount_cents
FROM (
    SELECT amount_cents
      FROM lab02.check_current
     WHERE dataset_id = {p_dataset_id:String}
       AND status = 'paid'
       AND is_deleted = 0
);
