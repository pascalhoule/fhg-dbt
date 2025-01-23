{{  config(
    alias='brokeradvanced_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']}) }}

SELECT *
FROM {{ ref('brokeradvanced_vc_clean_insurance') }}