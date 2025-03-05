{{  config(alias='hierarchy_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  

from {{ ref ('hierarchy_vc_integrate_insurance')  }}