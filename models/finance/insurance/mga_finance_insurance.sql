{{  config(alias='mga', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('mga_clean_insurance')  }}