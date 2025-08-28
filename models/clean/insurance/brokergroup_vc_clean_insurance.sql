 {{  config(alias='brokergroup_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokergroup_vc')  }}