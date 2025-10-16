 {{  config(alias='insuranceuser_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

FROM {{ source ('insurance_curated', 'insuranceuser_vc')  }}