{{ config( alias = 'commissiongrid_vc', database = 'agt_comm', schema = 'insurance', materialized = "view") }} 

SELECT * 
FROM {{ ref('commissiongrid_vc_analyze_insurance') }}