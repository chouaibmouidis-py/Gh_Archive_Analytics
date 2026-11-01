{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

SELECT
    event_id,
    event_type,
    event_at,
    event_date,
    actor_id,
    repo_id,
    org_id,
    is_public,
    payload,
    _ingested_at
FROM {{ ref('stg_events') }}

{% if is_incremental() %}
WHERE event_at > (SELECT MAX(event_at) FROM {{ this }})
{% endif %}