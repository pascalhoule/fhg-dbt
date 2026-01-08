{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_ws_fin', 			
        database='integrate', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_normalize_investment') }}