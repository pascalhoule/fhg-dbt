{{  config(alias='policystatuschanges', database='integrate', schema='insurance')  }} 


select *
from {{ ref ('policystatuschanges_normalize_insurance')  }} 