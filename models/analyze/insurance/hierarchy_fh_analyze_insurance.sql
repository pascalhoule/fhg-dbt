{{  
    config(
    alias='hierarchy_fh', 
    database='analyze', 
    schema='insurance')  
    }} 

SELECT *
FROM {{ ref('hierarchy_fh_integrate_insurance') }}