{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_transaction', 			
        database='integrate', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_transaction_normalize_investment') }}