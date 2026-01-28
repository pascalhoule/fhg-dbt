{{  
    config(alias='repmap_fh', 
    database='normalize', 
    schema='investment')  
}} 

WITH STEP_1 AS (
    SELECT
        REPRESENTATIVECODE,
        REPID,
        REPSTATUS,
        FIRST_NAME,
        LAST_NAME,
        COALESCE(
            NULLIF(
                TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(REP_COMM.FIRST_NAME), ''),
                        NULLIF(TRIM(REP_COMM.LAST_NAME), '')
                    )
                ),
                ''
            ),
            NULLIF(TRIM(REP_COMM.LAST_NAME), ''),
            NULLIF(TRIM(BCH_COMM.NAME), '')
        ) AS COMMISSION_REP_NAME,
        BCH_COMM.NAME AS BRANCH,
        SUBREG_COMM.NAME AS SUB_REGION
    FROM
        {{ ref('representatives_vc_normalize_investment') }} REP_COMM
        JOIN {{ ref('branches_vc_normalize_investment') }} BCH_COMM ON REP_COMM.BRANCH_CODE = BCH_COMM.CODE
        JOIN {{ ref('region_vc_normalize_investment') }} SUBREG_COMM ON SUBREG_COMM.SUBREGIONCODE = BCH_COMM.SUBREGIONCODE
),
B1 AS (
    SELECT
        S.*,
        CASE
            WHEN S.COMMISSION_REP_NAME ILIKE '% LOC%'
            OR S.COMMISSION_REP_NAME ILIKE '%DEPT%'
            OR LOWER(S.COMMISSION_REP_NAME) = 'vancouver island (victoria)' THEN S.COMMISSION_REP_NAME
            WHEN UPPER(S.COMMISSION_REP_NAME) LIKE '%MAP - W%' THEN 'WSE'
            WHEN UPPER(S.COMMISSION_REP_NAME) LIKE '%MAP - F%' THEN 'F55'
            ELSE NULL
        END AS BRANCH_1
    FROM
        STEP_1 S
),
B2 AS (
    SELECT
        B1.*,
        CASE
            WHEN BRANCH_1 in ('WSE', 'F55') THEN NULL
            WHEN SUB_REGION ILIKE '%Brokers%' THEN SUB_REGION
            ELSE NULL
        END AS BRANCH_2,
        COALESCE(BRANCH_1, BRANCH_2) AS BRANCH_3
    FROM
        B1
),
BRANCH_MAP AS (
    SELECT
        B2.*,
        BCH_MAP.FHG_BRANCH
    FROM
        B2
        LEFT JOIN {{ source('norm', 'location_fin_fh') }} BCH_MAP ON B2.BRANCH_3 = BCH_MAP.COMMISSION_REP_NAME
),
REGION_MAP AS (
    SELECT
        BRANCH_MAP.*,
        REG.REGION
    FROM
        BRANCH_MAP
        LEFT JOIN {{ source('norm', 'region_fin_fh') }} REG ON BRANCH_MAP.FHG_BRANCH = REG.BRANCH
)
SELECT
    REPRESENTATIVECODE,
    REPID,
    REPSTATUS,
    FIRST_NAME,
    LAST_NAME,
    COMMISSION_REP_NAME AS FULL_REP_NAME,
    BRANCH,
    SUB_REGION,
    FHG_BRANCH AS BRANCH_FH,
    REGION AS REGION_FH
FROM
    REGION_MAP