 {{  config(alias='policystatus_vc', database='integrate', schema='insurance')  }} 


select *
from {{ ref ('policystatus_vc_normalize_insurance')  }} 