 {{  config(alias='employee_vc', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('employee_vc_normalize_insurance')  }}