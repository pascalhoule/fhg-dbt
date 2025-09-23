{{ config( alias= 'commission_vc', database= 'report_cl', schema= 'insurance') }} 

SELECT *
FROM {{ ref('commission_vc_analyze_insurance') }}