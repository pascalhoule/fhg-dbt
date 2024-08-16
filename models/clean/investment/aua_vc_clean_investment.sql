 {{  config(alias='aua_vc', database='clean', schema='investment')  }} 


select *
from {{ source('investment_curated', 'aua_vc') }}


