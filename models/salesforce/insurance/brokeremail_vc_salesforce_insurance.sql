{{  config(
    alias='brokeremail_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']}) }}

SELECT *
FROM {{ ref('brokeremail_vc_clean_insurance') }}
