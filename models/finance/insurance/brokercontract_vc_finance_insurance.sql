{{ config(
    alias = 'brokercontract_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
   BROKERCONTRACTCODE, 
   AGENTCODE, 
   ICCODE, 
   CONTRACTNUMBER, 
   BROKERCONTRACTSTATUSCODE, 
   BROKERCONTRACTTYPECODE, 
   CREATEDDATE, 
   LICENSEDATE, 
   EXPIREDATE, 
   LASTMODIFIEDDATE
FROM {{ ref('brokercontract_vc_clean_insurance') }}