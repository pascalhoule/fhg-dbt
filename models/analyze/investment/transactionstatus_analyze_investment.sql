{{ config(
    alias='transactionstatus', 
    database='analyze', 
    schema='investment') }} 

SELECT
    *
FROM {{ ref('transactionstatus_integrate_investment') }}