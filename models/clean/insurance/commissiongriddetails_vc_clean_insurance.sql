{{  config(alias='commissiongriddetails_vc', database='clean', schema='insurance')  }} 

Select *
  
from {{ source ('insurance_curated', 'commissiongriddetails_vc')  }}