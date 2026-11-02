SELECT
    repo_id,
    event_date,
    COUNT(*) AS total_events,
    COUNT(event_date) FILTER(WHERE event_type = 'PushEvent') AS push_events,
    COUNT(event_date) FILTER(WHERE event_type = 'WatchEvent') AS watch_events,
    COUNT(event_date) FILTER(WHERE event_type = 'PullRequestEvent') AS pull_request_events,
    COUNT(DISTINCT actor_id) AS unique_actor_count,
FROM {{ ref('int_events_deduplicated') }}
GROUP BY repo_id, event_date
HAVING COUNT(*) > 1