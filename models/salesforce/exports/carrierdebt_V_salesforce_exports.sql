{{ config(
    alias='carrierdebt_V', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']})  }}


SELECT
    BCD.BROKERCONTRACTCODE,
    IC.ICNAME AS CARRIER_NAME,
    BCT.ENGLISHDESCRIPTION AS CONTRACT_TYPE,
    BC.CONTRACTNUMBER AS LICENSE_CODE,
    BCD.DEBTSTATUS AS STATUS,
    BC.AGENTCODE,
    CAST(BCD.BALANCE AS FLOAT) AS BALANCE,
    CAST(BCD.ACTIVATEDDATE AS DATE) AS ACTIVATED_DATE
FROM {{ ref('brokercarrierdebt_vc_salesforce_insurance') }} AS BCD
INNER JOIN {{ ref('brokercontract_vc_salesforce_insurance') }} AS BC
    ON BCD.BROKERCONTRACTCODE = BC.BROKERCONTRACTCODE
LEFT JOIN {{ ref('ic_vc_salesforce_insurance') }} AS IC
    ON BC.ICCODE = IC.ICCODE
LEFT JOIN {{ ref('brokercontracttype_vc_salesforce_insurance') }} AS BCT
    ON BC.BROKERCONTRACTTYPECODE = BCT.CODE
WHERE
    BCD.DEBTSTATUS = 'Active'
    AND BCD.BALANCE <> 0
