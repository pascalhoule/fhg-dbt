{{  config(alias='policy_vc', database='clean', schema='insurance', tags = ["policy_fh"])  }} 
 


SELECT * 
 
from {{ source ('insurance_curated', 'policy_vc')  }}