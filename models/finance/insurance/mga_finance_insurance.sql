{{  config(alias='mga', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('mga_clean_insurance')  }}