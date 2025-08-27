{{  config(alias='assistantemail_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'assistantemail_vc')  }}