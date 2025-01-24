{{ config(
    alias='provinciallicences_V', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT
    BCP.LICENSECODE,
    concat (B.FIRSTNAME,'',B.MIDDLENAME,'',B.LASTNAME)AGENTNAME,
    BCP.LICENSENUMBER,
    BCP.TYPE AS LICENSE_TYPE,
    BCP.CATEGORY,
    BCP.PROVINCE,
    BCP.AGENTCODE,
    COALESCE(BCP.ENDDATE, '9999-12-31') AS END_DATE
FROM {{ ref('brokercontractprovince_vc_salesforce_insurance') }} AS BCP
LEFT JOIN {{ ref('broker_V_salesforce_exports') }} AS B
    ON BCP.AGENTCODE = B.AGENTCODE
