{{ config( alias= 'commission_vc', database= 'analyze', schema= 'insurance') }} 

SELECT *
FROM {{ ref('commission_vc_integrate_insurance') }}