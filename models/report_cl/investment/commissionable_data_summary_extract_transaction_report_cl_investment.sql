{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_transaction', 			
        database='report_cl', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_transaction_analyze_investment') }}