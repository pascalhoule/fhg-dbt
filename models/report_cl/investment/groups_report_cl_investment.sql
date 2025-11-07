{{  config(
    alias='groups', 
    database='report_cl', 
    schema='investment')  }} 

SELECT * FROM {{ ref('groups_analyze_investment') }}