{{ config( alias = 'brokeradvanced_vc', database = 'agt_comm', schema = 'insurance', materialized = "view") }} 


SELECT
    AGENTCODE,
    USERDEFINED1,
    USERDEFINED2
FROM {{ ref ('brokeradvanced_vc_analyze_insurance') }}
