 {{  config(alias='employers_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

FROM {{ source ('insurance_curated', 'employers_vc')  }}