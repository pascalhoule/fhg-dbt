{{  config(alias='policytasks_vc', database='clean', schema='insurance') }} 


SELECT * 
  
  
from {{ source ('insurance_curated', 'policytasks_vc')  }}