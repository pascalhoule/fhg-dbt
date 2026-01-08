{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_transaction', 			
        database='analyze', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_transaction_integrate_investment') }}