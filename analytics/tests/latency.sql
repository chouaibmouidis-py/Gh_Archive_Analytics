{{ config(severity='warn') }}

SELECT
    event_id,
    _ingested_at,
    event_at
FROM {{ ref('int_events_deduplicated') }}
WHERE event_date <> partition_date
