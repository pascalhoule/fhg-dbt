{{ config( alias = 'hierarchy_vc', database = 'agt_comm', schema = 'insurance', materialized = "view") }} 

SELECT *
FROM {{ ref('hierarchy_vc_analyze_insurance') }}