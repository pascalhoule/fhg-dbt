{{  
    config(
    alias='hierarchy_fh', 
    database='report', 
    schema='daily_apps')  
    }} 

SELECT *
FROM {{ ref('hierarchy_fh_cl_integrate_insurance') }}