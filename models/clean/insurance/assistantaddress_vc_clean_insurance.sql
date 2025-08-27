{{  config(alias='assistantaddress_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
from {{ source ('insurance_curated', 'assistantaddress_vc')  }}