{{  config(alias='commissiongrid_vc', database='clean', schema='insurance')  }} 

Select *
  
from {{ source ('insurance_curated', 'commissiongrid_vc')  }}