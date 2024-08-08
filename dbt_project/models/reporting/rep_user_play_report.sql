{{ config(
    materialized='table'
) }}

WITH int_users_story AS(
    SELECT *
    FROM  {{ ref('int_users_story') }}
)

, final AS (
SELECT 
    user_id 
    , user_age_group 
    , subscription_status   
    , date_cet
    , COUNT(DISTINCT session_id)         AS session_count
    , SUM(play_duration_seconds)         AS total_play_duration
    , AVG(play_duration_seconds)         AS avg_play_duration
    , MAX(timestamp_cet)                 AS first_play_timestamp
    , MIN(timestamp_cet)                 AS last_play_timestamp           
FROM  int_users_story
GROUP BY 1, 2, 3, 4
)

SELECT * 
FROM final