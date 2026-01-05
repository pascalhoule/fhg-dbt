{{			
    config (			
        materialized="view",			
        alias='joint_id_rate_fh', 			
        database='normalize', 			
        schema='investment'  		
    )			
}}

WITH T1 AS (
    SELECT
        *,
        REPID AS INITIAL_ID 
    FROM
        {{ ref('representatives_vc_normalize_investment') }}
    WHERE
        LAST_NAME LIKE '%/%'
        OR FIRST_NAME LIKE '%/%'
),
T2 AS (
    SELECT
        INDIVIDUALREPRESENTATIVECODE,
        JOINTREPRESENTATIVECODE,
        SHARE,
        INITIAL_ID
    FROM
        {{ ref('jointrepresentatives_vc_normalize_investment') }} JR
        INNER JOIN T1 ON JOINTREPRESENTATIVECODE = REPRESENTATIVECODE
)
SELECT
    LAST_NAME,
    FIRST_NAME,
    REPID,
    INITIAL_ID AS JOINTID,
    SHARE
FROM
    {{ ref('representatives_vc_normalize_investment') }} RE
    INNER JOIN T2 ON RE.REPRESENTATIVECODE = T2.INDIVIDUALREPRESENTATIVECODE