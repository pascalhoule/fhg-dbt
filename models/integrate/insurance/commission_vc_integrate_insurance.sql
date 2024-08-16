{{ config( alias= 'commission_vc', database= 'integrate', schema= 'insurance') }} 

SELECT *
FROM {{ ref('commission_vc_normalize_insurance') }}