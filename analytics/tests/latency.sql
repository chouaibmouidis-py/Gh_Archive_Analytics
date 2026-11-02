{{ config(severity='warn') }}

SELECT
    event_id,
    _ingested_at,
    event_at
FROM {{ ref('fct_events') }}
WHERE event_date <> partition_date
