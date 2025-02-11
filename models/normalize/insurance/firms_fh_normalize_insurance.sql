{{			
    config (			
        materialized="view",			
        alias='firms_fh', 			
        database='normalize', 			
        schema='insurance'			
    )			
}}

SELECT
    NODEID,
    trim(FIRM) AS FIRM,
    IFF(CONTAINS(FIRM, '-FIRM'), TRUE, FALSE) AS Individual_Agent
FROM
    {{ ref('hierarchy_fh_normalize_insurance') }}
group by
    1, 2, 3