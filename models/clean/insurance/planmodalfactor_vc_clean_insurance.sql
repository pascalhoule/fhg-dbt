{{  config(alias='planmodalfactor_vc', database='clean', schema='insurance') }} 


SELECT * 
  
  
from {{ source ('insurance_curated', 'planmodalfactor_vc')  }}