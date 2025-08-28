 {{  config(alias='brokereo_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokereo_vc')  }}