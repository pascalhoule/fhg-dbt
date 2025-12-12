{{  config(alias='policyagentlinking', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('policyagentlinking_clean_insurance')  }}