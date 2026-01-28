{{			
    config (			
        materialized="view",			
        alias='repmap_fh', 			
        database='analyze', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('repmap_fh_integrate_investment') }}