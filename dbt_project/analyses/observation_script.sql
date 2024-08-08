--Observations:

-- 1. Toneis_playbacks_sample_data.csv data contain 61 unique users.
    SELECT
        COUNT(DISTINCT user_id)
     FROM TONIESDATASET.PUBLIC.TONIES_PLAYBACKS_SAMPLE_DATA

-- 2.  115 totak records in dataset before cleaning.
    SELECT
         COUNT(*)
    FROM TONIESDATASET.PUBLIC.TONIES_PLAYBACKS_SAMPLE_DATA

-- 3. 100 total record after cleaning duplicate records.
    WITH user_session_id AS (
        SELECT
            DISTINCT (CONCAT(user_id, SUBSTRING(session_id, 2))) AS user_session_id
            , *
        FROM TONIESDATASET.PUBLIC.TONIES_PLAYBACKS_SAMPLE_DATA
      )
    SELECT COUNT(session_id) AS session_id_count
    FROM user_session_id

-- 4. 1 user had 5 session_id, 2 users had 4 sessions_id and 4 users had 3 sessions_id
   
     WITH user_session_id AS (
        SELECT
            DISTINCT (CONCAT(user_id, SUBSTRING(session_id, 2))) AS user_session_id
             , *
        FROM TONIESDATASET.PUBLIC.TONIES_PLAYBACKS_SAMPLE_DATA
        )
    SELECT user_id,COUNT(session_id) AS session_id_count FROM user_session_id
    GROUP BY user_id