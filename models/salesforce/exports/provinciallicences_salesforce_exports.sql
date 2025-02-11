{{ config(
    alias='provinciallicences', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT
    BCP.LICENSECODE,
    B.BROKERNAME,
    BCP.LICENSENUMBER,
    BCP.TYPE AS LICENSE_TYPE,
    BCP.CATEGORY,
    BCP.PROVINCE,
    BCP.AGENTCODE,
    COALESCE(BCP.ENDDATE, '9999-12-31') AS END_DATE
FROM {{ ref('brokercontractprovince_vc_salesforce_insurance') }} AS BCP
LEFT JOIN {{ ref('broker_vc_salesforce_insurance') }} AS B
    ON BCP.AGENTCODE = B.AGENTCODE
