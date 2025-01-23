{{  config(
    alias='brokercarrierdebt_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']}) }}

SELECT *
FROM {{ ref('brokercarrierdebt_vc_clean_insurance') }}