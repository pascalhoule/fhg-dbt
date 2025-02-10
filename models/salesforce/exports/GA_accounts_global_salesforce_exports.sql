{{ config(
    alias='GA_accounts_global', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT
        '0128W000000awsHQAQ' AS Recordtypeid,
        B.Uid,
        B.Agentcode,
        B.Firstname,
        B.Middlename,
        B.Brokerid,
        B.Agenttype,
        B.Agentstatus,
        B.Servicelevel,
        B.Mgacode,
        B.Aganame,
        B.Emailaddress,
        B.Business_phone,
        B.Marketing_office,
        B.Companyname,
        B.Address,
        B.City,
        B.Province,
        B.Postal_code,
        B.Fax,
        B.Homephone,
        B.Cellphone,
        B.Alternatephone,
        B.Alternatedescription,
        B.Businessemail,
        B.Personalemail,
        B.Dateofbirth,
        B.Languagepreference,
        B.Notification,
        B.Localdelivery,
        B.Outofcity,
        B.LOCATION,
        B.Region,
        B.Segment,
        B.Undersupervision,
        B.Ismap,
        B.Pendingtermination,
        TRUE AS Isprimary,
        B.Tagname,
        CONCAT(B.Lastname, ' - GA/CG') AS Lastname,
        CONCAT(B.Firstname, ' ', B.Lastname, ' - GA/CG') AS Name,
        (SELECT COUNT(*) FROM Broker WHERE Uid = B.Uid) AS Profilecount
    FROM
        {{ ref('broker_salesforce_exports') }} AS B
    WHERE
        B.Uid = B.Brokerid
)

SELECT * FROM Temp
