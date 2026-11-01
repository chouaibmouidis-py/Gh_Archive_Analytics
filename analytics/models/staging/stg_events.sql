WITH source AS (

    SELECT * FROM {{ source('bronze', 'events') }}

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY _ingested_at DESC
    ) = 1

)

SELECT
    id                                      AS event_id,
    type                                    AS event_type,
    created_at                              AS event_at,
    CAST(created_at AS DATE)                AS event_date,
    json_extract(actor, '$.id')::BIGINT     AS actor_id,
    json_extract_string(actor, '$.login')   AS actor_login,
    json_extract(repo, '$.id')::BIGINT      AS repo_id,
    json_extract_string(repo, '$.name')     AS repo_name,
    json_extract(org, '$.id')::BIGINT       AS org_id,
    json_extract_string(org, '$.login')     AS org_login,
    public                                  AS is_public,
    payload,
    _source_file,
    _ingested_at
FROM deduplicated