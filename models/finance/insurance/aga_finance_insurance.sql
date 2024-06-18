{{  config(alias='aga', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('aga_clean_insurance')  }}