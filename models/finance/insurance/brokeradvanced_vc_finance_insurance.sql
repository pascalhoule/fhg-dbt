{{ config(
    alias = 'brokeradvanced_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
    AGENTCODE,
    USERDEFINED1,
    USERDEFINED2
FROM {{ ref('brokeradvanced_vc_clean_insurance') }}
