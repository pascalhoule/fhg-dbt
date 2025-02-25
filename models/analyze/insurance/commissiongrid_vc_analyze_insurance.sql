{{  config(alias='commissiongrid_vc', database='analyze', schema='insurance')  }} 

SELECT *
FROM {{ ref('commissiongrid_vc_integrate_insurance') }}