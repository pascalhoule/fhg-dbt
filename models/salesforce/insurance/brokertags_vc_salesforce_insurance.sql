{{  config(
    alias='brokertags_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})  }}

SELECT *
FROM {{ ref('brokertags_vc_clean_insurance') }}