{{  config(alias='brokermarketingoffice_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokermarketingoffice_vc')  }}