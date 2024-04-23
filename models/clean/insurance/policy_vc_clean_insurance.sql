{{  config(alias='policy_vc', database='clean', schema='insurance', tags = ["policy_fh"])  }} 
 


SELECT * ,
 current_timestamp AS updated_at

 
from {{ source ('insurance_curated', 'policy_vc')  }}