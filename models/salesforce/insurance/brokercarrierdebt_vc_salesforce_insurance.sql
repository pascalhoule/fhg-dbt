{{  config(
    alias='brokercarrierdebt_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ source('insurance_curated', 'brokercarrierdebt_vc') }}