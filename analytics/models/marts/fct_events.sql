{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

{% if is_incremental() and execute %}
    {% set max_date = run_query("select max(event_date) from " ~ this).columns[0].values()[0] %}
{% endif %}

SELECT
    event_id,
    event_type,
    event_at,
    event_date,
    actor_id,
    repo_id,
    org_id,
    is_public,
    repo_name,
    actor_login,
    org_login,
    partition_date,
    _ingested_at
FROM {{ ref('stg_events') }}

{% if is_incremental() %}
WHERE partition_date >= CAST('{{ max_date }}' AS DATE)
{% else %}
WHERE partition_date = '2024-01-15'
{% endif %}
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY event_id
    ORDER BY _ingested_at DESC
) = 1