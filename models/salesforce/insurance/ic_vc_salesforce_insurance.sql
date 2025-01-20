{{  config(
    alias='ic_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('ic_vc_clean_insurance') }}