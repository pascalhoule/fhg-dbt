{{ config(
    alias='transactionstatus', 
    database='integrate', 
    schema='investment') }} 

SELECT
    *
FROM {{ ref('transactionstatus_normalize_investment') }}
