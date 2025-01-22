{{  config(
    alias='brokerphone_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokerphone_vc_clean_insurance') }}