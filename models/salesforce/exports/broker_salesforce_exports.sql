{{ config(
    alias='broker', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT
    BADV.USERDEFINED2 AS UID,
    B.AGENTCODE,
    B.AGENTTYPE,
    B.AGENTSTATUS,
    B.MGACODE,
    RH.NODENAME AS AGANAME,
    BPHONE.NUMBER AS BUSINESS_PHONE,
    '' AS MARKETING_OFFICE,
    FAX.NUMBER AS FAX,
    HOME.NUMBER AS HOMEPHONE,
    CELL.NUMBER AS CELLPHONE,
    ALTERNATE.NUMBER AS ALTERNATEPHONE, -- Primary email validated
    '' AS ALTERNATEDESCRIPTION,
    B.DATEOFBIRTH,
    B.LANGUAGEPREFERENCE,
    '' AS NOTIFICATION,
    '' AS LOCALDELIVERY,
    '' AS OUTOFCITY,
    COALESCE(B.FIRSTNAME, '') AS FIRSTNAME,
    COALESCE(B.MIDDLENAME, '') AS MIDDLENAME,
    CASE
        WHEN
            COALESCE(B.FIRSTNAME, '') = ''
            AND COALESCE(B.MIDDLENAME, '') = ''
            AND COALESCE(B.LASTNAME, '') = ''
            THEN COALESCE(B.COMPANYNAME, '')
        ELSE B.LASTNAME
    END AS LASTNAME,
    COALESCE(B.BROKERID, '') AS BROKERID,
    COALESCE(B.SERVICELEVEL, '') AS SERVICELEVEL,
    CASE
        WHEN POSITION('@' IN E.EMAILADDRESS) > 0 THEN E.EMAILADDRESS
    END AS EMAILADDRESS,
    COALESCE(B.COMPANYNAME, '') AS COMPANYNAME, -- Business email validated
    COALESCE(BA_BUSINESS.ADDRESS, '') AS ADDRESS, -- Personal email validated
    COALESCE(BA_BUSINESS.CITY, '') AS CITY,
    COALESCE(BA_BUSINESS.PROVINCE, '') AS PROVINCE,
    COALESCE(BA_BUSINESS.POSTAL_CODE, '') AS POSTAL_CODE,
    CASE
        WHEN POSITION('@' IN BE.EMAILADDRESS) > 0 THEN BE.EMAILADDRESS
    END AS BUSINESSEMAIL,
    CASE
        WHEN POSITION('@' IN PE.EMAILADDRESS) > 0 THEN PE.EMAILADDRESS
    END AS PERSONALEMAIL,
    COALESCE(H.LOCATION, 'Unknown Branch') AS LOCATION,
    CASE
    SUBSTR(MAX(RH.HIERARCHYPATH), 1, 4)
        WHEN '^m1^' THEN 'Quebec'
        WHEN '^m2^' THEN 'West'
        WHEN '^m3^' THEN 'Ontario Non GTA and Atlantic'
        WHEN '^m4^' THEN 'Ontario GTA and CAM'
    END AS REGION,
   CAST(
        REPLACE(
            REPLACE(
                REPLACE(
                    NVL((
                        SELECT MAX(BT.TAGNAME) 
                        FROM FH_PROD.WEALTHSERV_INS_CURATED_SECURE.BROKERTAGS_VC BT 
                        WHERE BT.TAGNAME IN (
                            'Segment A',
                            'Segment B',
                            'Segment C'
                        ) 
                        AND BT.AGENTCODE = B.AGENTCODE
                    ), ''),
                    'Segment A', 'Segment A'
                ), 
                'Segment B', 'Segment B'
            ), 
            'Segment C', 'Segment C'
        ) AS VARCHAR
    ) AS SEGMENT,
    MAX(CASE
        WHEN BT.TAGNAME = 'Under Supervision/Sous supervision' THEN 1
        ELSE 0
    END) AS UNDERSUPERVISION,
    MAX(CASE
        WHEN BT.TAGNAME = 'Pending Termination/En attente de résiliation' THEN 1
        ELSE 0
    END) AS PENDINGTERMINATION,
    CASE
        WHEN COUNT(DISTINCT BT.TAGNAME) = 1 THEN MAX(BT.TAGNAME)
        ELSE
            LISTAGG(DISTINCT BT.TAGNAME, '; ') WITHIN GROUP (
                ORDER BY BT.TAGNAME
            )
    END AS TAGNAME
FROM REPORT.PROD_INSURANCE.BROKER_FH AS B
LEFT JOIN {{ ref('brokeradvanced_vc_salesforce_insurance') }} AS BADV
    ON B.AGENTCODE = BADV.AGENTCODE
LEFT JOIN {{ ref('brokeremail_vc_salesforce_insurance') }} AS E
    ON B.AGENTCODE = E.AGENTCODE AND E.TYPE = 'primary' -- Primary email
LEFT JOIN {{ ref('brokeremail_vc_salesforce_insurance') }} AS BE
    ON B.AGENTCODE = BE.AGENTCODE AND BE.TYPE = 'business' -- Business email
LEFT JOIN {{ ref('brokeremail_vc_salesforce_insurance') }} AS PE
    ON B.AGENTCODE = PE.AGENTCODE AND PE.TYPE = 'personal' -- Personal email
LEFT JOIN {{ ref('brokeraddress_vc_salesforce_insurance') }} AS BA_BUSINESS
    ON B.AGENTCODE = BA_BUSINESS.AGENTCODE AND BA_BUSINESS.TYPE = 'business'
LEFT JOIN {{ ref('brokerphone_vc_salesforce_insurance') }} AS BPHONE
    ON B.AGENTCODE = BPHONE.AGENTCODE AND BPHONE.TYPE = 'Business'
LEFT JOIN {{ ref('brokerphone_vc_salesforce_insurance') }} AS FAX
    ON B.AGENTCODE = FAX.AGENTCODE AND FAX.TYPE = 'Company Fax'
LEFT JOIN {{ ref('brokerphone_vc_salesforce_insurance') }} AS HOME
    ON B.AGENTCODE = HOME.AGENTCODE AND HOME.TYPE = 'Home'
LEFT JOIN {{ ref('brokerphone_vc_salesforce_insurance') }} AS CELL
    ON B.AGENTCODE = CELL.AGENTCODE AND CELL.TYPE = 'Cell'
LEFT JOIN {{ ref('brokerphone_vc_salesforce_insurance') }} AS ALTERNATE
    ON B.AGENTCODE = ALTERNATE.AGENTCODE AND ALTERNATE.TYPE = 'Alternate'
LEFT JOIN {{ ref('hierarchy_fh_salesforce_insurance') }} AS H
    ON TRIM(B.PARENTNODEID) = TRIM(H.NODEID)
LEFT JOIN {{ ref('recursivehierarchy_vc_salesforce_insurance') }} AS RH
    ON TRIM(B.PARENTNODEID) = TRIM(RH.NODEID)
LEFT JOIN {{ ref('brokertags_vc_salesforce_insurance') }} AS BT
    ON B.AGENTCODE = BT.AGENTCODE
WHERE
    LEFT(BADV.USERDEFINED2, 4) IN ('3162', '3268')
    AND LENGTH(BADV.USERDEFINED2) <= 10
    AND B.AGENTSTATUS NOT IN ('Deleted', 'Service Only')
    AND B.AGENTTYPE <> 'Financial Horizons'
    AND B.MGACODE IS NOT NULL
GROUP BY
    BADV.USERDEFINED2,
    B.AGENTCODE,
    B.FIRSTNAME,
    B.MIDDLENAME,
    B.LASTNAME,
    B.BROKERID,
    B.AGENTTYPE,
    B.AGENTSTATUS,
    B.SERVICELEVEL,
    B.COMPANYNAME,
    B.MGACODE,
    B.AGACODE,
    E.EMAILADDRESS,
    BPHONE.NUMBER,
    FAX.NUMBER,
    HOME.NUMBER,
    CELL.NUMBER,
    ALTERNATE.NUMBER,
    BE.EMAILADDRESS,
    PE.EMAILADDRESS,
    BA_BUSINESS.ADDRESS,
    BA_BUSINESS.CITY,
    BA_BUSINESS.PROVINCE,
    BA_BUSINESS.POSTAL_CODE,
    B.DATEOFBIRTH,
    B.LANGUAGEPREFERENCE,
    H.LOCATION,
    RH.NODENAME
