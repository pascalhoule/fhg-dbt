{{ config(
    alias='carriercontract', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}


SELECT
    BC.BROKERCONTRACTCODE,
    IC.ICNAME,
    BCT.ENGLISHDESCRIPTION AS CONTRACT_TYPE,
    BC.CONTRACTNUMBER,
    BCS.ENGLISHDESCRIPTION AS CONTRACT_STATUS,
    BC.AGENTCODE
FROM {{ ref('brokercontract_vc_salesforce_insurance') }} AS BC
LEFT JOIN {{ ref('ic_vc_salesforce_insurance') }} AS IC ON BC.ICCODE = IC.ICCODE
LEFT JOIN
    {{ ref('brokercontracttype_vc_salesforce_insurance') }} AS BCT
    ON BC.BROKERCONTRACTTYPECODE = BCT.CODE
LEFT JOIN
    {{ ref('brokercontractstatus_vc_salesforce_insurance') }} AS BCS
    ON BC.BROKERCONTRACTSTATUSCODE = BCS.CODE
