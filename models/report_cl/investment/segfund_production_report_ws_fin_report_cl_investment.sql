{{			
    config (			
        materialized="view",			
        alias='segfund_production_report_ws_fin', 			
        database='report_cl', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('segfund_production_report_WS_fin_analyze_investment') }}