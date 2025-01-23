{{  config(
    alias='hierarchy_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})   }}

SELECT *
FROM {{ ref('hierarchy_vc_clean_insurance') }}