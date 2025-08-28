{{  config(alias='brokergroupmembership_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'brokergroupmembership_vc')  }}