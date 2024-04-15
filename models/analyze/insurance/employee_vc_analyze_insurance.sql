{{  config(alias='employee_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('employee_vc_integrate_insurance')  }}