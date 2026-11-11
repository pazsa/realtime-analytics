SELECT
    concat(replace(toString(now64(3, 'UTC')), ' ', 'T'), 'Z') AS computed_at,
    if(
        maxOrNull(event_time) IS NULL,
        NULL,
        concat(
            replace(
                toString(maxOrNull(event_time)),
                ' ',
                'T'
            ),
            'Z'
        )
    ) AS latest_event_at
FROM check_raw
WHERE dataset_id = {dataset_id:String}
;
