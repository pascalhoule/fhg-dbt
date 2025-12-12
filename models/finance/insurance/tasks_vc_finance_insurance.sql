{{  config(alias='tasks_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('tasks_vc_clean_insurance')  }}