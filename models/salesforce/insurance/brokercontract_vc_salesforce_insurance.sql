{{  config(
    alias='brokercontract_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokercontract_vc_clean_insurance') }}