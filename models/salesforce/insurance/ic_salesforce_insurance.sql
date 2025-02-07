{{  config(
    alias='ic', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('ic_clean_insurance') }}