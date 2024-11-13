{{  config(alias='firm', 
database='finance', 
schema='queries', 
materialization = "view")  }} 

WITH FF55_MAP AS (
        SELECT
            *,
            SPLIT_PART(HIERARCHYPATHNAME, '>>', 5) AS BRANCH_NAME,
            TRIM(SPLIT_PART(HIERARCHYPATHNAME, '>>', 6)) AS FIRM_NAME
        FROM
            {{ ref('recursive_hierarchy_finance_insurance') }}
        WHERE
            HIERARCHYLEVEL = 6
            AND (
                CONTAINS(HIERARCHYPATHNAME, 'MAP - Freedom')
                OR CONTAINS(HIERARCHYPATHNAME, 'MAP - WISE')
                OR CONTAINS(HIERARCHYPATHNAME, 'People Corporation')
            )
    ),
    H_LEVEL5 AS (
        SELECT
            *,
            SPLIT_PART(HIERARCHYPATHNAME, '>>', 4) AS BRANCH_NAME,
            TRIM(SPLIT_PART(HIERARCHYPATHNAME, '>>', 5)) AS FIRM_NAME
        FROM
            {{ ref('recursive_hierarchy_finance_insurance') }}
        WHERE
            HIERARCHYLEVEL = 5
            AND NODEID NOT IN (
                SELECT
                    DISTINCT NODEID
                FROM
                    FF55_MAP
            )
    )
SELECT
    *
FROM
    H_LEVEL5
WHERE
    NOT (
        CONTAINS(FIRM_NAME, 'MAP - WISE')
        OR CONTAINS(FIRM_NAME, 'MAP - Freedom')
    )
UNION
SELECT
    *
FROM
    FF55_MAP