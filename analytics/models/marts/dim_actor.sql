SELECT
    actor_id,
    ARG_MAX(actor_login, event_at)  AS actor_login,
    MIN(event_at)                   AS first_event_at,
    MAX(event_at)                   AS last_event_at,
    COUNT(*)                        AS event_count
FROM {{ ref('int_events_deduplicated') }}
GROUP BY actor_id