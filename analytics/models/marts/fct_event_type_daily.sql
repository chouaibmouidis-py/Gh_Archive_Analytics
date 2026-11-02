SELECT
    event_date,
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT actor_id) AS unique_actor_count,
    COUNT(DISTINCT repo_id) AS unique_repo_count
FROM {{ ref('int_events_deduplicated') }}
GROUP BY event_date, event_type