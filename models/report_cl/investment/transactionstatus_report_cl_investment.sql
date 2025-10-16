{{ config(
    alias='transactionstatus', 
    database='report_cl', 
    schema='investment') }} 

SELECT
    *
FROM {{ ref('transactionstatus_analyze_investment') }}