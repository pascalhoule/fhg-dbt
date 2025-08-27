{{ config(
    alias = 'brokercontractprovince_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
   LICENSECODE,
   LICENSENUMBER,
   STARTDATE,
   ENDDATE,
   AGENTCODE,
   TYPE,
   CATEGORY,
   PROVINCE,
   LASTMODIFIEDDATE
FROM {{ ref('brokercontractprovince_vc_clean_insurance') }}