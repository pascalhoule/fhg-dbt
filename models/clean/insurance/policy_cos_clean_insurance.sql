{{  config(alias='policycos_vc', database='clean', schema='insurance')  }} 
 


SELECT * ,
 current_timestamp AS updated_at

 
from {{ source ('insurance_curated', 'policycos_vc')  }}