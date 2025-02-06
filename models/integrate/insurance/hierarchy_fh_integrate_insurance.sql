{{  
    config(
    alias='hierarchy_fh', 
    database='integrate', 
    schema='insurance')  
    }} 

SELECT
    H.*,
    M.NEW_SEGMENT AS SEGMENT
FROM
    {{ ref('hierarchy_fh_normalize_insurance') }} H
    LEFT JOIN {{ source('dimensions', 'agent_firm_segment_map') }} M ON H.BROKERID = M.BROKERID