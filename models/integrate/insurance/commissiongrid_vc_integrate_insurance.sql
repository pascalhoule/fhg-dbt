{{  config(alias='commissiongrid_vc_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
FROM {{ ref ('commissiongrid_vc_normalize_insurance')  }}