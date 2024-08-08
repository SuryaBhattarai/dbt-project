{{ 
    config(
        materialized='table'
    ) 
}}

WITH raw_sample_data AS(
    SELECT *
    FROM  {{ ref('raw_sample_data') }}
)
    /*  - Dublication of the data with same user_id and session_id; create user_session_id column
        and filter distinct records
        - There are some records with same session_id; I did not filtered it out by assuming
        two or more users sharing the same session */
, without_duplicate AS (                                        -- remove duplicates
    SELECT 
        DISTINCT(CONCAT(user_id, SUBSTRING(session_id, 2))) AS user_session_id
        , *
    FROM raw_sample_data
)

  -- Data Cleaning and Transformaton 
, final AS (
    SELECT 
        user_id                                                   AS user_id
        , SUBSTRING(session_id, 2)                                AS session_id
        , user_session_id                                         AS user_session_id
        , ABS(COALESCE(play_duration_seconds, 0))                 AS play_duration_seconds
        ,CASE                                                     -- add 0 after 'st',when story_id had 1 digit after st
            WHEN LENGTH(SUBSTRING(story_id, 3)) = 1 THEN '0' || SUBSTRING(story_id, 3)
            ELSE SUBSTRING(story_id, 3)
        END                                                       AS story_id
        , COALESCE(                                              -- timestamp format
            TO_CHAR(
                CASE
                    WHEN TRY_TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL
                        THEN TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')
                    WHEN TRY_TO_TIMESTAMP(timestamp, 'MM/DD/YYYY HH24:MI') IS NOT NULL
                        THEN TO_TIMESTAMP(timestamp, 'MM/DD/YYYY HH24:MI')
                END,
                'YYYY-MM-DD HH24:MI:SS'
            ),
            '0'
          )                                                       AS timestamp_utc
        , CASE                                                   -- new mapping and data correction
            WHEN user_age_group = '0-3' THEN '0-3'
            WHEN user_age_group IN ('4-7', '8-12', '5-12') THEN '4-12'
            WHEN user_age_group = 'teen' THEN '13-19'
            ELSE 'unknown'
          END                                                     AS user_age_group
        , CASE 
            WHEN subscription_status IN ('activ', 'active') THEN 'active'
            WHEN subscription_status = 'expired' THEN 'expired'
            ELSE 'unknown'
          END                                                     AS subscription_status
    FROM without_duplicate
)
SELECT 
    user_id
    , session_id
    , user_session_id
    , play_duration_seconds
    , story_id
    , CONVERT_TIMEZONE('Europe/Berlin', timestamp_utc) AS timestamp_cet -- convert utc to cet timezone
    , user_age_group
    , subscription_status
FROM final

{% if is_incremental() %}
WHERE timestamp_cet > (SELECT MAX(timestamp_cet) FROM {{ this }})
{% endif %}