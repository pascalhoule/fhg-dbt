{{  
    config(
    alias='hierarchy_fh', 
    database='report', 
    schema='in_the_mill',
    grants = {'ownership': ['FH_READER']},
    tags=["in_the_mill"])  
    }} 

SELECT *
FROM {{ ref('hierarchy_fh_report_insurance') }}