{{ 
    config(
        materialized='table'
    )
}}

WITH ods_sample_data AS(
    SELECT *
    FROM  {{ ref('ods_sample_data') }}
)

, final AS (
SELECT 
    user_id                     AS user_id
    , user_age_group            AS user_age_group
    , subscription_status       AS  subscription_status

FROM  ods_sample_data
)

SELECT * 
FROM final
