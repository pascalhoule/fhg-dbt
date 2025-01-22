{{  config(
    alias='brokertags_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokertags_vc_clean_insurance') }}