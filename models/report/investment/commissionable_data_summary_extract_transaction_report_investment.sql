{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_transaction', 			
        database='report', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_transaction_analyze_investment') }}