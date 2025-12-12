{{  config(alias='usergroup_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('usergroup_vc_clean_insurance')  }}