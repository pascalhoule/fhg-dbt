{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_ws_fin', 			
        database='analyze', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('commissionable_data_summary_extract_ws_fin_integrate_investment') }}