 {{  config(alias='employee_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

from {{ source ('insurance_curated', 'employee_vc')  }}