{{			
    config (			
        materialized="view",			
        alias='segbudget_fin', 			
        database='report', 			
        schema='segfund',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

select * from {{ ref('segbudget_fin') }}