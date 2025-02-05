{{  
    config(
    alias='hierarchy_fh', 
    database='report', 
    schema='sales')  
    }} 

SELECT *
FROM {{ ref('hierarchy_fh_report_insurance') }}