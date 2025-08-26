 {{  config(alias='assistant_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'assistant_vc')  }}