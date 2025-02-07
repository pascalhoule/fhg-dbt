{{  config(
    alias='icagent', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('icagent_clean_insurance') }}