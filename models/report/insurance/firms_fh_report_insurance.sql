{{  config(
    materialized = 'view',
    alias='firms_fh', 
    database='report', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']},	)  }} 

SELECT
    NODEID,
    FIRM,
    IFF(CONTAINS(FIRM, '-FIRM'), TRUE, FALSE) AS INDIVIDUAL_AGENT
FROM
    {{ ref('hierarchy_fh_analyze_insurance') }}
GROUP BY
    1, 2, 3