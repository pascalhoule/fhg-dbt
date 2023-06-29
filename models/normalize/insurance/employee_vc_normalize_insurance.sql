 {{  config(alias='employee_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('employee_vc_clean_insurance')  }}