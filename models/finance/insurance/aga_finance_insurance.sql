{{  config(alias='aga', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('aga_clean_insurance')  }}