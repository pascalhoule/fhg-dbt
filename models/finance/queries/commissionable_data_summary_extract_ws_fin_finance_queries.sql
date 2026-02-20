{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_ws_fin', 			
        database='finance', 			
        schema='queries'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_normalize_investment') }}