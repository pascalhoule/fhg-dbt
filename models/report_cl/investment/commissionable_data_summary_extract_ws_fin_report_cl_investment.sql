{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_ws_fin', 			
        database='report_cl', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_ws_fin_analyze_investment') }}