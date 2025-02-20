{{			
    config (			
        materialized="table",			
        alias='broker_fh', 			
        database='integrate', 			
        schema='insurance'			
    )			
}}	

WITH EMAIL_AGT_LIST AS (
        SELECT
            DISTINCT AGENTCODE
        FROM
            {{ ref('brokeremail_vc_normalize_insurance') }}
    ),
    EMAIL_BUSINESS AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            {{ ref('brokeremail_vc_normalize_insurance') }}
        WHERE
            TYPE = 'business'
    ),
    EMAIL_PRIMARY AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            {{ ref('brokeremail_vc_normalize_insurance') }}
        WHERE
            TYPE = 'primary'
    ),
    EMAIL_SECONDARY AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            {{ ref('brokeremail_vc_normalize_insurance') }}
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
            B.FIRSTNAME,
            B.MIDDLENAME,
            B.LASTNAME,
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
            {{ ref('broker_vc_normalize_insurance') }} B
            JOIN {{ ref('brokeradvanced_vc_normalize_insurance') }} BA ON B.AGENTCODE = BA.AGENTCODE
            LEFT JOIN EMAIL ON EMAIL.AGENTCODE = B.AGENTCODE
    ),
    SEGMENT_TAGS AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME IN (
                'Segment A',
                'Segment B',
                'Segment C'
            )
    ),
    PIECES_OF_SEGMENT AS (
        SELECT
            AGT_LIST.*,
            CASE
                WHEN CONTAINS(BT.TAGNAME, 'Segment A') THEN 'Segment A'
                ELSE NULL
            END AS SegmentA,
            CASE
                WHEN BT.TAGNAME = 'Segment B' THEN 'Segment B'
                ELSE NULL
            END AS SegmentB,
            CASE
                WHEN BT.TAGNAME = 'Segment C' THEN 'Segment C'
                ELSE NULL
            END AS SegmentC
        FROM
            AGT_LIST
            LEFT JOIN SEGMENT_TAGS BT ON AGT_LIST.AGENTCODE = BT.AGENTCODE
    ),
    AAP AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME = 'AAP'
    ),
    ADSL AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME = 'ADSL'
    ),
    ELEVATE AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME = 'Elevated/élevée'
    ),
    PENDTERM AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME = 'Pending Termination/En attente de résiliation'
    ),
    TERM AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME = 'Terminated/Terminé'
    ),
    TFEROUT AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            {{ ref('brokertags_vc_normalize_insurance') }}
        WHERE
            TAGNAME = 'Transferring Out/Transfert Sortant'
    ),
    COS_RSC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Sales Coordinator'
    ),
    COS_BDC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Business Development Consultant'
    ),
    COS_BDD AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Business Development Director'
    ),
    COS_SVP AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Senior Vice President'
    ),
    COS_RVP AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Vice President'
    ),
    COS_VP AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Vice President'
    ),
    COS_CC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Contracting Coordinator'
    ),
    COS_CS AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        where
            ROLE = 'Contracting Specialist'
    ),
    COS_RCM AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Compliance Manager'
    ),
    COS_BOC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Branch Office Coordinator'
    ),
    COS_NBS_INV AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'New Business Specialist - Investments'
    ),
    COS_NBS_CM AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'New Business Specialist - Case Manager'
    ),
    COS_NBS_INF AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'New Business Specialist - Inforce'
    ),
    COS_ROM AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
           {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Operations Manager'
    ),
    COS_RAM AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Administration Manager'
    ),
    COS_IS AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Insurance Strategist'
    ),
    COS_WS AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
           {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Wealth Strategist'
    ),
    COS_RMO AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Relationship Manager - Operations'
    ),
    COS_RMCC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            {{ ref('brokercos_vc_normalize_insurance') }} COS
            LEFT JOIN {{ ref('employee_vc_normalize_insurance') }} EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Relationship Manager - Contracting and Compensation'
    ),
    CONSOLIDATED_SEGMENT AS (
        SELECT
            SEG.*,
            CASE
                WHEN SEG.SegmentA IS NOT NULL THEN 'Segment A'
                WHEN SEG.SegmentB IS NOT NULL THEN 'Segment B'
                WHEN SEG.SegmentC IS NOT NULL THEN 'Segment C'
                ELSE NULL
            END AS SEGMENTTAGWS,
            CASE
                WHEN AAP.TAGNAME = 'AAP' THEN 'AAP'
            END AS AAP_TAG,
            CASE
                WHEN ADSL.TAGNAME = 'ADSL' THEN 'ADSL'
            END AS ADSL_TAG,
            CASE
                WHEN ELEVATE.TAGNAME = 'Elevated/élevée' THEN 'Elevated'
            END AS ELEVATED,
            CASE
                WHEN PENDTERM.TAGNAME = 'Pending Termination/En attente de résiliation' THEN 'Pending Termination'
            END AS PENDINGTERMINATION,
            CASE
                WHEN TERM.TAGNAME = 'Terminated/Terminé' THEN 'Terminated'
            END AS TERMINATED,
            CASE
                WHEN TFEROUT.TAGNAME = 'Transferring Out/Transfert Sortant' THEN 'Transferring Out'
            END AS TRANSFERRINGOUT,
            CASE
                WHEN RSC.ROLE = 'Regional Sales Coordinator' THEN RSC.FULLNAME
            END AS COS_SALES_RSC,
            CASE
                WHEN BDC.ROLE = 'Business Development Consultant' THEN BDC.FULLNAME
            END AS COS_SALES_BDC,
            CASE
                WHEN COS_BDD.ROLE = 'Business Development Director' THEN COS_BDD.FULLNAME
            END AS COS_SALES_BDD,
            CASE
                WHEN COS_SVP.ROLE = 'Senior Vice President' THEN COS_SVP.FULLNAME
            END AS COS_SALES_SVP,
            CASE
                WHEN COS_RVP.ROLE = 'Regional Vice President' THEN COS_RVP.FULLNAME
            END AS COS_SALES_RVP,
            CASE
                WHEN COS_VP.ROLE = 'Vice President' THEN COS_VP.FULLNAME
            END AS COS_SALES_VP,
            CASE
                WHEN COS_CC.ROLE = 'Contracting Coordinator' THEN COS_CC.FULLNAME
            END AS COS_CONTRACT_CC,
            CASE
                WHEN COS_CS.ROLE = 'Contracting Specialist' THEN COS_CS.FULLNAME
            END AS COS_CONTRACT_CS,
            CASE
                WHEN COS_RCM.ROLE = 'Regional Compliance Manager' THEN COS_RCM.FULLNAME
            END AS COS_COMPLIANCE_RCM,
            CASE
                WHEN COS_BOC.ROLE = 'Branch Office Coordinator' THEN COS_BOC.FULLNAME
            END AS COS_OPS_BOC,
            CASE
                WHEN COS_NBS_INV.ROLE = 'New Business Specialist - Investments' THEN COS_NBS_INV.FULLNAME
            END AS COS_OPS_NBS_INV,
            CASE
                WHEN COS_NBS_CM.ROLE = 'New Business Specialist - Case Manager' THEN COS_NBS_INV.FULLNAME
            END AS COS_OPS_NBS_CM,
            CASE
                WHEN COS_NBS_INF.ROLE = 'New Business Specialist - Inforce' THEN COS_NBS_INV.FULLNAME
            END AS COS_OPS_NBS_INF,
            CASE
                WHEN COS_ROM.ROLE = 'Regional Operations Manager' THEN COS_NBS_INV.FULLNAME
            END AS COS_OPS_ROM,
            CASE
                WHEN COS_RAM.ROLE = 'Regional Administration Manager' THEN COS_RAM.FULLNAME
            END AS COS_RAM,
            CASE
                WHEN COS_IS.ROLE = 'Insurance Strategist' THEN COS_IS.FULLNAME
            END AS COS_SALESIS,
            CASE
                WHEN COS_WS.ROLE = 'Wealth Strategist' THEN COS_WS.FULLNAME
            END AS COS_SALES_WS,
            CASE
                WHEN COS_RMO.ROLE = 'Relationship Manager - Operations' THEN COS_RMO.FULLNAME
            END AS COS_OPS_RMO,
            CASE
                WHEN COS_RMCC.ROLE = 'Relationship Manager - Contracting and Compensation' THEN COS_RMCC.FULLNAME
            END AS COS_CONTRACT_RMCC
        FROM
            PIECES_OF_SEGMENT SEG
            LEFT JOIN AAP ON SEG.AGENTCODE = AAP.AGENTCODE
            LEFT JOIN ADSL ON SEG.AGENTCODE = ADSL.AGENTCODE
            LEFT JOIN ELEVATE ON SEG.AGENTCODE = ELEVATE.AGENTCODE
            LEFT JOIN PENDTERM ON SEG.AGENTCODE = PENDTERM.AGENTCODE
            LEFT JOIN TERM ON SEG.AGENTCODE = TERM.AGENTCODE
            LEFT JOIN TFEROUT ON SEG.AGENTCODE = TFEROUT.AGENTCODE
            LEFT JOIN COS_RSC RSC ON RSC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_BDC BDC ON BDC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_BDD ON COS_BDD.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_SVP ON COS_SVP.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_RVP ON COS_RVP.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_VP ON COS_VP.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_CC ON COS_CC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_CS ON COS_CS.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_RCM ON COS_RCM.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_BOC ON COS_BOC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_NBS_INV ON COS_NBS_INV.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_NBS_CM ON COS_NBS_CM.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_NBS_INF ON COS_NBS_INF.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_ROM ON COS_ROM.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_RAM ON COS_RAM.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_IS ON COS_IS.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_WS ON COS_WS.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_RMO ON COS_RMO.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_RMCC ON COS_RMCC.AGENTCODE = SEG.AGENTCODE
    )
    SELECT
        *
    FROM
        CONSOLIDATED_SEGMENT
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
