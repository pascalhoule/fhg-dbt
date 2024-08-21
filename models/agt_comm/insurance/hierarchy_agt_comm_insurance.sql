{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='agt_comm', 			
        schema='insurance'			
    )			
}}	

SELECT *,
SUBSTR(NODEID, 2, LEN(NODEID)-2) AS MODIFIED_NODE_ID
FROM {{ ref ('hierarchy_analyze_insurance')  }}