{{  config(
    alias='brokercontract_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('brokercontract_vc_clean_insurance') }}