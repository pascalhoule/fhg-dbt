{{			
    config (			
        materialized="view",			
        alias='segfund_production_report_transaction', 			
        database='finance', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('segfund_production_report_transaction_normalize_investment') }}