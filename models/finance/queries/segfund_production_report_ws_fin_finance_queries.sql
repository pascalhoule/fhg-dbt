{{			
    config (			
        materialized="view",			
        alias='segfund_production_report_ws_fin', 			
        database='finance', 			
        schema='queries'  		
    )			
}}

SELECT * FROM {{ ref('segfund_production_report_normalize_investment') }}