{{  config(
    alias='brokeradvanced_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokeradvanced_vc_clean_insurance') }}