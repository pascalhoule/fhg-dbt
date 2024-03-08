 {{  config(alias='brokerphone_vc', database='normalize', schema='insurance')  }} 

SELECT
    AGENTCODE,
    TYPE,
    NUMBER 
FROM {{ ref ('brokerphone_vc_clean_insurance')  }}