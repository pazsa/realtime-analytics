SELECT order_id
     , campaign_id
     , version
     , status
     , amount_cents
     , created_at
     , event_time
  FROM get_order_data(p_dataset_id={dataset_id:String}, p_order_id={order_id:String})

