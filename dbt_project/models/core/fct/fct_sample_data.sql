{{
  config(
    materialized='incremental',
    unique_key='user_session_id'  
  )
}}

WITH ods_sample_data AS(
    SELECT *
    FROM  {{ ref('ods_sample_data') }}
)

, final AS (
SELECT 
    user_id                     AS user_id
    , session_id                AS session_id
    , user_session_id           AS user_session_id
    , play_duration_seconds     AS play_duration_seconds
    , story_id                  AS story_id
    , timestamp_cet             AS timestamp_cet 

FROM  ods_sample_data
)

SELECT * 
FROM final

{% if is_incremental() %}
WHERE timestamp_cet > (SELECT MAX(timestamp_cet) FROM {{ this }})
{% endif %}
