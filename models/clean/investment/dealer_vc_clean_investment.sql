 {{  config(alias='dealer_vc', database='clean', schema='investment')  }} 


select *
from {{ source('investment_curated', 'dealer_vc') }}