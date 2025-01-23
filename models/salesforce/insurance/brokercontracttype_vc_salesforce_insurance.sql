{{  config(
    alias='brokercontracttype_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('brokercontracttype_vc_clean_insurance') }}