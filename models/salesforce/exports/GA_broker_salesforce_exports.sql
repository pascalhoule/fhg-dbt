{{ config(
    alias='GA_broker', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT
        Uid,
        Agentcode,
        Firstname,
        Middlename,
        Lastname,
        Brokerid,
        Agenttype,
        Agentstatus,
        Servicelevel,
        Mgacode,
        Aganame,
        Business_phone,
        Marketing_office,
        Companyname,
        Address,
        City,
        Province,
        Postal_code,
        Fax,
        Homephone,
        Cellphone,
        Alternatephone,
        Alternatedescription,
        Dateofbirth,
        Languagepreference,
        Notification,
        Localdelivery,
        Outofcity,
        Branchname,
        Region,
        Segment,
        Undersupervision,
        Ismap,
        Pendingtermination,
        Tagname,
        CASE
            WHEN
                REGEXP_LIKE(
                    Emailaddress,
                    '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                )
                THEN Emailaddress
            ELSE ''
        END AS Emailaddress,
        CASE
            WHEN
                REGEXP_LIKE(
                    Businessemail,
                    '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                )
                THEN Businessemail
            ELSE ''
        END AS Businessemail,
        CASE
            WHEN
                REGEXP_LIKE(
                    Personalemail,
                    '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                )
                THEN Personalemail
            ELSE ''
        END AS Personalemail
    FROM {{ ref('broker_salesforce_exports') }}
)

SELECT * FROM Temp
