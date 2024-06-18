{{  config(alias='policy_vc', database='finance', schema='insurance') }} 


SELECT * 
  
  
from {{ ref('policy_vc_clean_insurance') }}