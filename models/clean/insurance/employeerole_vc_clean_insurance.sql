 {{  config(alias='employeerole_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

from {{ source ('insurance_curated', 'employeerole_vc')  }}