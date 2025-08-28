{{  config(alias='clienttype_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'clienttype_vc')  }}