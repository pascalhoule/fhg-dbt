{{  config(alias='broker_emailtracking', 
    database='applications', 
    schema='Emailtracking',
    materialized = "table",
    grants = {'ownership': ['FH_READER']})  }} 


SELECT 
    B.AGENTCODE,
    COALESCE(B.BROKERID, '') AS BROKERID,
    COALESCE(LOWER(MAX(CASE WHEN BE.TYPE = 'business' THEN BE.EMAILADDRESS END)), '') AS BUSINESS_EMAILADDRESS,
    COALESCE(LOWER(MAX(CASE WHEN BE.TYPE = 'primary' THEN BE.EMAILADDRESS END)), '') AS PERSON1_EMAILADDRESS,
    COALESCE(LOWER(MAX(CASE WHEN BE.TYPE = 'secondary' THEN BE.EMAILADDRESS END)), '') AS PERSON2_EMAILADDRESS,
    B.AGENTSTATUS
FROM {{ ref('broker_fh_exports_applications') }} B
LEFT JOIN {{ ref('brokeremail_vc_exports_applications') }} BE ON BE.AGENTCODE = B.AGENTCODE
GROUP BY B.AGENTCODE, B.BROKERID, B.AGENTSTATUS
