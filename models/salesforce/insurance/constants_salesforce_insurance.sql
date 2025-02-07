{{  config(
    alias='constants', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref ('constants_clean_insurance') }}