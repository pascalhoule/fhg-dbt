{{  config(alias='client_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'client_vc')  }}