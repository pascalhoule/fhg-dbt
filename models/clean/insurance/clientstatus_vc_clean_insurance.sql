{{  config(alias='clientstatus_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'clientstatus_vc')  }}