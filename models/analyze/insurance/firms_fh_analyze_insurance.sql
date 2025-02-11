{{			
    config (			
        materialized="view",			
        alias='firms_fh', 			
        database='analyze', 			
        schema='insurance'			
    )			
}}

SELECT
    NODEID,
    FIRM,
    IFF(CONTAINS(FIRM, '-FIRM'), TRUE, FALSE) AS INDIVIDUAL_AGENT
FROM
    {{ ref('hierarchy_fh_integrate_insurance') }}
GROUP BY
    1, 2, 3