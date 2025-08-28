{{  config(alias='brokertasks_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokertasks_vc')  }}