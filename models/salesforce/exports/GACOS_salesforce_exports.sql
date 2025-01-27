{{ config(
    alias='GACOS_V', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT DISTINCT
    BC.ROLE,
    BC.EMPLOYEENAME,
    '' AS EMPLOYEEEMAIL,
    TRIM(BA.USERDEFINED2) AS GA_UID,
    CONCAT('uid_', TRIM(BA.USERDEFINED2), '_', TRIM(BC.ROLECODE)) AS UID
FROM {{ ref('brokercos_vc_salesforce_insurance') }} AS BC
LEFT JOIN {{ ref('brokeradvanced_vc_salesforce_insurance') }} AS BA
    ON BC.AGENTCODE = BA.AGENTCODE
WHERE
    LEFT(BA.USERDEFINED2, 4) IN ('3162', '3268')
    AND LENGTH(BA.USERDEFINED2) < 10
ORDER BY TRIM(BA.USERDEFINED2), TRIM(BC.ROLE)
