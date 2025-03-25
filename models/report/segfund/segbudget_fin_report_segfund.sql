{{			
    config (			
        materialized="view",			
        alias='segbudget_fin', 			
        database='report', 			
        schema='segfund',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

SELECT 
    AdvisorID,
    AdvisorName,
    Region,
    RevType,
    RevCategory,
    RevTransType,
    Period,
    Year1,
    Amount,
    Source,
    Ledger
FROM {{ ref('segbudget_fin') }}