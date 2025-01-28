{{  config(
    alias='brokeraddress_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})   }}

SELECT *
FROM {{ ref('brokeraddress_vc_clean_insurance') }}
