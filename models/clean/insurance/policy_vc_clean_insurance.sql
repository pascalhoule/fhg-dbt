{{  config(alias='policy_vc', database='clean', schema='insurance')  }} 
 


SELECT * 
 
from {{ source ('insurance_curated', 'policy_vc')  }}