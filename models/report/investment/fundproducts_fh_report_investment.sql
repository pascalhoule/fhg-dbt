{{			
    config (			
        materialized="view",			
        alias='fundproducts_fh', 			
        database='report', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('fundproducts_fh_analyze_investment') }}