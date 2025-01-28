{{  config(
    alias='brokercontractprovince_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']}) }}

SELECT *
FROM {{ ref('brokercontractprovince_vc_clean_insurance') }}