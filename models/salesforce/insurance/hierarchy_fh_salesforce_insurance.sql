{{  config(
    alias='hierarchy_fh', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})   }}

SELECT *
FROM {{ ref('hierarchy_fh_normalize_insurance') }}