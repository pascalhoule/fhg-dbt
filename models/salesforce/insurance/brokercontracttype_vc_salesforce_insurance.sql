{{  config(
    alias='brokercontracttype_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokercontracttype_vc_clean_insurance') }}