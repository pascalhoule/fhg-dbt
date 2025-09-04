{{  config(alias='policybridge_vc', database='clean', schema='insurance', tags = ["policy_fh"])  }} 
 


SELECT * ,
 current_timestamp AS updated_at

 
from {{ source ('insurance_curated', 'policybridge_vc')  }}