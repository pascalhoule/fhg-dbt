{{  config(alias='policy_vc', database='integrate', schema='insurance')  }} 


select *
from {{ ref ('policy_vc_normalize_insurance')  }} 