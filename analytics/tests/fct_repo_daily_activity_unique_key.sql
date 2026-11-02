SELECT repo_id, event_date, COUNT(*) AS n
FROM {{ ref('fct_repo_daily_activity') }}
GROUP BY repo_id, event_date
HAVING COUNT(*) > 1