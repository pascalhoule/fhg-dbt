{{  
    config(
    alias='hierarchy_fh', 
    database='report', 
    schema='insurance')  
    }} 

SELECT *
FROM {{ ref('hierarchy_fh_analyze_insurance') }}