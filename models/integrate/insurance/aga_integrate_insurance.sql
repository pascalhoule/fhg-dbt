{{  config(alias='aga', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('aga_normalize_insurance')  }}