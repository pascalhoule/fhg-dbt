{{  
    config(
    alias='hierarchy_fh', 
    database='report', 
    schema='sales',
    grants = {'ownership': ['FH_READER']},	
    tags=["sales", "large_case"])  
    }} 

SELECT *
FROM {{ ref('hierarchy_fh_report_insurance') }}