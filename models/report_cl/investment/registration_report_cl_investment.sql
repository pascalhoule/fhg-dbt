{{  config(
    alias='registration', 
    database='report_cl', 
    schema='investment')  }} 

SELECT * FROM {{ ref('registration_analyze_investment') }}