{{  config(alias='policyrider_vc', database='clean', schema='insurance') }} 


SELECT * 
  
  
from {{ source ('insurance_curated', 'policyrider_vc')  }}