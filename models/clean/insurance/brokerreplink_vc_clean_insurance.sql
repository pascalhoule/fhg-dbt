{{  config(alias='brokerreplink_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokerreplink_vc')  }}