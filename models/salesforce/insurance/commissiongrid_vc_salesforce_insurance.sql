{{  config(
    alias='commissiongrid_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})   }}

SELECT *
FROM {{ ref('commissiongrid_vc_clean_insurance') }}