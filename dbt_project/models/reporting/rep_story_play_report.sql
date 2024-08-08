{{ config(
    materialized='table'
) }}

WITH int_sample_users_story AS(
    SELECT *
    FROM  {{ ref('int_sample_users_story') }}
)

, final AS (
SELECT 
    story_id   
    , date_cet
    , COUNT(DISTINCT user_id)           AS unique_users
    , COUNT(session_id)                 AS session_count
    , SUM(play_duration_seconds)        AS total_play_duration
    , AVG(play_duration_seconds)        AS avg_play_duration
FROM  int_sample_users_story
GROUP BY 1, 2
)

SELECT * 
FROM final
