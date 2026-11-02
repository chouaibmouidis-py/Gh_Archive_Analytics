SELECT
    repo_id,
    ARG_MAX(repo_name, event_at)  AS repo_name,
    MIN(event_at)                 AS first_event_at,
    MAX(event_at)                 AS last_event_at,
    COUNT(*)                      AS event_count
FROM {{ ref('int_events_deduplicated') }}
GROUP BY repo_id