{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='report', 			
        schema='in_the_mill',
        tags=["in_the_mill"]			
    )			
}}

SELECT
        NODEID,
        SPLIT_PART(HIERARCHYPATH, '|', 3) AS BRANCHID,
        BRANCHNAME,
        REGION,
        SPLIT_PART(HIERARCHYPATHNAME, '>>', 2) AS LEVEL2,
        SPLIT_PART(HIERARCHYPATHNAME, '>>', 3) AS LEVEL3,
        SPLIT_PART(HIERARCHYPATHNAME, '>>', 4) AS LEVEL4,
        SPLIT_PART(HIERARCHYPATHNAME, '>>', 5) AS LEVEL5,
        SPLIT_PART(HIERARCHYPATHNAME, '>>', 6) AS LEVEL6,
        SPLIT_PART(HIERARCHYPATHNAME, '>>', 7) AS LEVEL7
    FROM
        {{ ref('hierarchy_report_insurance') }}
    WHERE
        NODEID IS NOT NULL
    GROUP BY
        1,2,3,4,5,6,7,8,9,10