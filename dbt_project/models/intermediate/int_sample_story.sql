{{ config(
    materialized='view'
) }}

WITH  fct_sample_data AS (
    SELECT * 
    FROM {{ ref('fct_sample_data') }}
),

dim_sample_users AS (
    SELECT * 
    FROM {{ ref('dim_sample_users') }}
),

dim_sample_story AS (
    SELECT * 
    FROM {{ ref('dim_sample_story') }}
)
, final AS (
    SELECT
        fct_sample.user_id                      AS user_id
        , fct_sample.session_id                 AS session_id
        , fct_sample.user_session_id            AS user_session_id
        , fct_sample.play_duration_seconds      AS play_duration_seconds
        , dim_story.story_id                    AS story_id
        , fct_sample.timestamp_cet              AS timestamo_cet
        ,fct_sample.timestamp_cet::date         AS date_cet
        , dim_users.user_age_group              AS user_age_group
        , dim_users.subscription_status         AS subscription_status
    FROM fct_sample_data AS fct_sample
    LEFT JOIN dim_sample_users AS dim_users
        ON fct_sample.user_id = user_id.user_id
    LEFT JOIN dim_sample_story AS dim_story
        ON fct_sample.story_id = dim_story.story_id
)

SELECT *
FROM final
