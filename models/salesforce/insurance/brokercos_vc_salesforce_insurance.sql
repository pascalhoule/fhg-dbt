{{  config(
    alias='brokercos_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

select *
from {{ ref('brokercos_vc_integrate_insurance') }}