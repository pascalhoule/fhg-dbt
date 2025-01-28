{{  config(
    alias='brokerphone_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']}) }}

SELECT *
FROM {{ ref('brokerphone_vc_clean_insurance') }}