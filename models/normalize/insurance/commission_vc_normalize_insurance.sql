{{ config( alias= 'commission_vc', database= 'normalize', schema= 'insurance') }} 

SELECT *
FROM {{ ref('commission_vc_clean_insurance') }}
