{{  config(alias='hierarchy_vc', database='integrate', schema='insurance')  }} 

SELECT *
FROM {{ ref('hierarchy_vc_normalize_insurance') }}