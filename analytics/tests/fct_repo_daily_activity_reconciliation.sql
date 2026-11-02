SELECT 
    mart_total,
    int_total
FROM (
    SELECT
        (SELECT SUM(total_events) FROM {{ ref('fct_repo_daily_activity') }}) AS mart_total,
        (SELECT COUNT(*) FROM {{ ref('int_events_deduplicated') }}) AS int_total
)
WHERE mart_total <> int_total