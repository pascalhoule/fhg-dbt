{{  config(
    alias='brokercontractstatus_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('brokercontractstatus_vc_clean_insurance') }}