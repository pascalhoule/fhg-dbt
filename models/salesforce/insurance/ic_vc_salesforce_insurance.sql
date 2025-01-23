{{  config(
    alias='ic_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('ic_vc_clean_insurance') }}