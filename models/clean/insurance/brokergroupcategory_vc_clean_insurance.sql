 {{  config(alias='brokergroupcategory_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokergroupcategory_vc')  }}