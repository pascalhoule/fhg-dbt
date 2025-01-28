{{ config(
    alias='brokercos', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']})  }}

SELECT DISTINCT
    CONCAT('uid_', TRIM(AGENTCODE), '_', TRIM(ROLECODE)) AS UID,	
    ROLE AS ROLE, 
    EMPLOYEENAME AS EMPLOYEENAME, 
    AGENTCODE AS AGENTCODE
FROM {{ ref('brokercos_vc_salesforce_insurance') }}
ORDER BY AGENTCODE, ROLE