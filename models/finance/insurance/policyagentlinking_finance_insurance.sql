{{  config(alias='policyagentlinking', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('policyagentlinking_clean_insurance')  }}