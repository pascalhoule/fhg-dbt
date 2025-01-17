{{  config(
    alias='brokercarrierdebt_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokercarrierdebt_vc_clean_insurance') }}