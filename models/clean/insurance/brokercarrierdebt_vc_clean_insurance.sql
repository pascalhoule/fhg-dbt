{{  config(
    alias='brokercarrierdebt_vc', 
    database='clean', 
    schema='insurance')  }}

SELECT *
FROM {{ source('insurance_curated', 'brokercarrierdebt_vc') }}