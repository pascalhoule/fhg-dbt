{{			
    config (			
        materialized="view",			
        alias='fundproducts_fh', 			
        database='report_cl', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('fundproducts_fh_analyze_investment') }}