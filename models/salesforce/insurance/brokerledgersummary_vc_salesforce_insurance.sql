{{ config(
    alias='brokerledgersummary_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']}) }}

SELECT *
FROM {{ ref('brokerledgersummary_vc_clean_insurance') }}
