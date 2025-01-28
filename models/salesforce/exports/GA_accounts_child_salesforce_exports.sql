{{ config(
    alias='GA_accounts_child', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS
(
SELECT
    b.UID,
    b.AGENTCODE,
    b.FIRSTNAME,
    b.MIDDLENAME,
    b.LASTNAME,
    b.BROKERID,
    b.AGENTTYPE,
    b.AGENTSTATUS,
    b.SERVICELEVEL,
    b.MGACODE,
    b.AGANAME,
    b.EMAILADDRESS,
    b.BUSINESS_PHONE,
    b.MARKETING_OFFICE,
    b.COMPANYNAME,
    b.ADDRESS,
    b.CITY,
    b.PROVINCE,
    b.POSTAL_CODE,
    b.FAX,
    b.HOMEPHONE,
    b.CELLPHONE,
    b.ALTERNATEPHONE,
    b.ALTERNATEDESCRIPTION,
    b.BUSINESSEMAIL,
    b.PERSONALEMAIL,
    b.DATEOFBIRTH,
    b.LANGUAGEPREFERENCE,
    b.NOTIFICATION,
    b.LOCALDELIVERY,
    b.OUTOFCITY,
    b.BRANCHNAME,
    b.REGION,
    b.SEGMENT,
    b.UNDERSUPERVISION,
    b.ISMAP,
    b.PENDINGTERMINATION,
    CASE WHEN b.UID = b.BROKERID THEN TRUE ELSE FALSE END AS ISPRIMARY,
    (SELECT COUNT(*) FROM {{ ref('broker_salesforce_exports') }} WHERE UID = b.UID) AS PROFILECOUNT,
    b.TAGNAME
FROM
    {{ ref('broker_salesforce_exports') }} b
ORDER BY b.UID
)

select * from Temp
