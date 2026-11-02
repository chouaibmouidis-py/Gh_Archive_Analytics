SELECT event_date, event_type, COUNT(*) AS n
FROM {{ ref('fct_event_type_daily') }}
GROUP BY event_date, event_type
HAVING COUNT(*) > 1