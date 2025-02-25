{{ config(
    alias='GACOS', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT
    GA_UID,
    UID,
    ROLE,
    EMPLOYEENAME
FROM (
    SELECT DISTINCT
        BC.ROLE,
        BC.EMPLOYEENAME,
        TRIM(BA.USERDEFINED2) AS GA_UID,
        CONCAT('uid_', TRIM(BA.USERDEFINED2), '_', TRIM(BC.ROLECODE)) AS UID,
        ROW_NUMBER() OVER (PARTITION BY BC.ROLE, GA_UID, UID ORDER BY UID) AS R
    FROM {{ ref('brokercos_vc_salesforce_insurance') }} AS BC
    LEFT JOIN {{ ref('brokeradvanced_vc_salesforce_insurance') }} AS BA
        ON BC.AGENTCODE = BA.AGENTCODE
    WHERE
        LEFT(BA.USERDEFINED2, 4) IN ('3162', '3268')
        AND LENGTH(BA.USERDEFINED2) < 10
) AS FILTEREDDATA
WHERE R = 1
ORDER BY GA_UID, ROLE
