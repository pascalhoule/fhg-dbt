{{  config(alias='carrierplan_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'carrierplan_vc')  }}