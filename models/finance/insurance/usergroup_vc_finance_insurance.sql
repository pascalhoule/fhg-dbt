{{  config(alias='usergroup_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('usergroup_vc_clean_insurance')  }}