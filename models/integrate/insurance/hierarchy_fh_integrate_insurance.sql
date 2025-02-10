{{  
    config(
    alias='hierarchy_fh', 
    database='integrate', 
    schema='insurance')  
    }} 

SELECT
    *
FROM
    {{ ref('hierarchy_fh_normalize_insurance') }} 