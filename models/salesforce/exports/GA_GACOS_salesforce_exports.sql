{{ config(
    alias='GA_GACOS', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT DISTINCT
        Bc.Role,
        Bc.Employeename,
        TRIM(Ba.Userdefined2) AS Ga_uid,
        CONCAT('uid_', TRIM(Ba.Userdefined2), '_', TRIM(Bc.Rolecode)) AS Uid
    FROM {{ ref('brokercos_vc_salesforce_insurance') }} AS Bc
    LEFT JOIN {{ ref('brokeradvanced_vc_salesforce_insurance') }} AS Ba
        ON Bc.Agentcode = Ba.Agentcode
    WHERE
        LEFT(Ba.Userdefined2, 4) IN ('3162', '3268')
        AND LENGTH(Ba.Userdefined2) < 10
    ORDER BY TRIM(Ba.Userdefined2), TRIM(Bc.Role)
)

SELECT * FROM Temp
