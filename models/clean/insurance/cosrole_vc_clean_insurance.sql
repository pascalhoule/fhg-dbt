 {{  config(alias='cosrole_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

from {{ source ('insurance_curated', 'cosrole_vc')  }}