{{  config(
    alias='brokeraddress_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokeraddress_vc_clean_insurance') }}
