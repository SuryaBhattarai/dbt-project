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
    story_id                    AS story_id
    -- we can add other columns if available on tonies_story table; eg:story_name, author etc
  --  , story_name              AS story_name
  --  , author                  AS  author

FROM  ods_sample_data
)

SELECT * 
FROM final
