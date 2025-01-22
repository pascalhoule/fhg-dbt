{{  config(
    alias='brokercontractstatus_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokercontractstatus_vc_clean_insurance') }}