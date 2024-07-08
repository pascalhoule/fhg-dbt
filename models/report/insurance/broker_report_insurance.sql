{{			
    config (			
        materialized="view",			
        alias='broker', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

 WITH HIER AS (
        SELECT
            AGENTCODE,
            USERDEFINED2,
            MAP_SEGMENT
        FROM
            {{ ref ('hierarchy_report_insurance')  }}
        GROUP BY
            1,2,3
    ),
    EMAIL_AGT_LIST AS (
        SELECT
            DISTINCT AGENTCODE
        FROM
            {{ ref ('brokeremail_vc_report_insurance')  }}
    ),
    EMAIL_BUSINESS AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            {{ ref ('brokeremail_vc_report_insurance')  }}
        WHERE
            TYPE = 'business'
    ),
    EMAIL_PRIMARY AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            {{ ref ('brokeremail_vc_report_insurance')  }}
        WHERE
            TYPE = 'primary'
    ),
    EMAIL_SECONDARY AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            {{ ref ('brokeremail_vc_report_insurance')  }}
        WHERE
            TYPE = 'secondary'
    ),
    EMAIL AS (
        SELECT
            AGT.AGENTCODE,
            BUS.EMAILADDRESS AS BUSINESS_EMAIL,
            BUS.CASLAPPROVED AS BUSINESS_CASLAPPROVED,
            PRI.EMAILADDRESS AS PRIMARY_EMAIL,
            PRI.CASLAPPROVED AS PRIMARY_CASLAPPROVED,
            SEC.EMAILADDRESS AS SECONDARY_EMAIL,
            SEC.CASLAPPROVED AS SECONDARY_CASLAPPROVED,
        FROM
            EMAIL_AGT_LIST AGT
            LEFT JOIN EMAIL_BUSINESS BUS ON BUS.AGENTCODE = AGT.AGENTCODE
            LEFT JOIN EMAIL_PRIMARY PRI ON PRI.AGENTCODE = AGT.AGENTCODE
            LEFT JOIN EMAIL_SECONDARY SEC ON SEC.AGENTCODE = AGT.AGENTCODE
    ),
    AGT_LIST AS (
        SELECT
            CONCAT('^', B.PARENTNODEID, '^') AS PARENTNODEID,
            B.AGENTCODE,
            B.AGENTNAME,
            B.COMPANYNAME,
            B.MGACODE,
            B.AGACODE,
            B.DATEOFBIRTH,
            B.PROVINCE,
            B.COMPANYPROVINCE,
            B.AGENTSTATUS,
            B.AGENTTYPE,
            B.LANGUAGEPREFERENCE,
            B.SERVICELEVEL,
            B.CREATEDDATE,
            B.BROKERID,
            B.LASTMODIFIEDDATE,
            BA.USERDEFINED2,
            COALESCE(BUSINESS_EMAIL, PRIMARY_EMAIL, SECONDARY_EMAIL) AS EMAIL,
            COALESCE(
                BUSINESS_CASLAPPROVED,
                PRIMARY_CASLAPPROVED,
                SECONDARY_CASLAPPROVED
            ) AS CASLAPPROVED
        FROM
            {{ ref ('broker_vc_report_insurance')  }} B
            JOIN {{ ref ('brokeradvanced_vc_report_insurance')  }} BA ON B.AGENTCODE = BA.AGENTCODE
            LEFT JOIN EMAIL ON EMAIL.AGENTCODE = B.AGENTCODE
            --WHERE b.AGENTSTATUS in ('Active', 'Pending')
    ),
    SEGMENT_TAGS AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref ('brokertags_vc_report_insurance')  }}
        WHERE
            TAGNAME IN (
                'Advisor/Conseiller',
                'Select',
                'Signature',
                'Elite/Élite',
                'MAP-Advisor/PAM-Conseiller',
                'MAP-Select/PAM-Sélect',
                'MAP-Signature/PAM-Signature',
                'MAP-Elite/PAM-Élite'
            )
    ),
    PIECES_OF_SEGMENT AS (
        SELECT
            AGT_LIST.*,
            HIER.MAP_SEGMENT,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'Advisor/Conseiller') THEN 'Advisor'
                ELSE NULL
            END AS Advisor,
            CASE
                WHEN BT.TAGNAME = 'Select' THEN 'Select'
                ELSE NULL
            END AS Select_Agt,
            CASE
                WHEN BT.TAGNAME = 'Signature' THEN 'Signature'
                ELSE NULL
            END AS Signature_Agt,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'Elite/Élite') THEN 'Elite'
                ELSE NULL
            END AS Elite_Agt,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'MAP-Advisor/PAM-Conseiller') THEN 'MAP-Advisor'
                ELSE NULL
            END AS MAP_Advisor,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'MAP-Select/PAM-Sélect') THEN 'MAP-Select'
                ELSE NULL
            END AS MAP_Select,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'MAP-Signature/PAM-Signature') THEN 'MAP-Signature'
                ELSE NULL
            END AS MAP_Signature,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'MAP-Elite/PAM-Élite') THEN 'MAP-Elite'
                ELSE NULL
            END AS MAP_Elite
        FROM
            AGT_LIST
            LEFT JOIN HIER ON AGT_LIST.AGENTCODE = HIER.AGENTCODE
            LEFT JOIN SEGMENT_TAGS BT ON AGT_LIST.AGENTCODE = BT.AGENTCODE
    ),
    ELEVATE AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref ('brokertags_vc_report_insurance')  }}
        WHERE
            TAGNAME = 'Elevated/élevée'
    ),
    PENDTERM AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref ('brokertags_vc_report_insurance')  }}
        WHERE
            TAGNAME = 'Pending Termination/En attente de résiliation'
    ),
    TERM AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref ('brokertags_vc_report_insurance')  }}
        WHERE
            TAGNAME = 'Terminated/Terminé'
    ),
    TFEROUT AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref ('brokertags_vc_report_insurance')  }}
        WHERE
            TAGNAME = 'Transferring Out/Transfert Sortant'
    ),
    COS_RSC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref ('brokercos_vc_report_insurance')  }} COS
            LEFT JOIN {{ ref ('employee_vc_report_insurance')  }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Sales Coordinator'
    ),
    COS_BDC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref ('brokercos_vc_report_insurance') }} COS
            LEFT JOIN {{ ref ('employee_vc_report_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Business Development Consultant'
    ),
    COS_BDC_MAP AS (
        SELECT
            *,
            CONCAT(ee.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref ('brokercos_vc_report_insurance') }} COS
            LEFT JOIN {{ ref ('employee_vc_report_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Business Development Consultant - MAP'
    ),
    COS_SD AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref ('brokercos_vc_report_insurance') }} COS
            LEFT JOIN {{ ref ('employee_vc_report_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Sales Director'
    ),
    CONSOLIDATED_SEGMENT AS (
        SELECT
            SEG.*,
            CASE
                WHEN SEG.Elite_Agt IS NOT NULL THEN 'Elite'
                WHEN SEG.MAP_Elite IS NOT NULL THEN 'MAP-Elite'
                WHEN SEG.Select_Agt IS NOT NULL THEN 'Select'
                WHEN SEG.MAP_Select IS NOT NULL THEN 'MAP-Select'
                WHEN SEG.Signature_Agt IS NOT NULL THEN 'Signature'
                WHEN SEG.MAP_Signature IS NOT NULL THEN 'MAP-Signature'
                WHEN SEG.Advisor IS NOT NULL THEN 'Advisor'
                WHEN SEG.MAP_Advisor IS NOT NULL THEN 'MAP-Advisor'
                ELSE NULL
            END AS SEGMENTTAGWS,
            CASE
                WHEN ELEVATE.TAGNAME = 'Elevated/élevée' THEN 'Elevated'
            END AS Elevated,
            CASE
                WHEN PENDTERM.TAGNAME = 'Pending Termination/En attente de résiliation' THEN 'Pending Termination'
            END AS PENDINGTERMINATION,
            CASE
                WHEN TERM.TAGNAME = 'Terminated/Terminé' THEN 'Terminated'
            END AS Terminated,
            CASE
                WHEN TFEROUT.TAGNAME = 'Transferring Out/Transfert Sortant' THEN 'Transferring Out'
            END AS TRANSFERRINGOUT,
            CASE
                WHEN RSC.ROLE = 'Regional Sales Coordinator' THEN RSC.FullName
            END AS RSC,
            CASE
                WHEN BDC.ROLE = 'Business Development Consultant' THEN BDC.FullName
            END AS BDC,
            CASE
                WHEN MAP.ROLE = 'Business Development Consultant - MAP' THEN MAP.FullName
            END AS BDC_MAP,
            CASE
                WHEN SD.ROLE = 'Sales Director' THEN SD.FullName
            END AS SD
        FROM
            PIECES_OF_SEGMENT SEG
            LEFT JOIN ELEVATE ON SEG.AGENTCODE = ELEVATE.AGENTCODE
            LEFT JOIN PENDTERM ON SEG.AGENTCODE = PENDTERM.AGENTCODE
            LEFT JOIN TERM ON SEG.AGENTCODE = TERM.AGENTCODE
            LEFT JOIN TFEROUT ON SEG.AGENTCODE = TFEROUT.AGENTCODE
            LEFT JOIN COS_RSC RSC ON RSC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_BDC BDC ON BDC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_BDC_MAP MAP ON MAP.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_SD SD ON SD.AGENTCODE = SEG.AGENTCODE
    )
    SELECT
        *
    FROM
        CONSOLIDATED_SEGMENT
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37