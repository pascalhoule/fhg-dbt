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
            report.prod_insurance.hierarchy
        GROUP BY
            1,2,3
    ),
    EMAIL_AGT_LIST AS (
        SELECT
            DISTINCT AGENTCODE
        FROM
            report.prod_insurance.brokeremail_vc
    ),
    EMAIL_BUSINESS AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            report.prod_insurance.brokeremail_vc
        WHERE
            TYPE = 'business'
    ),
    EMAIL_PRIMARY AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            report.prod_insurance.brokeremail_vc
        WHERE
            TYPE = 'primary'
    ),
    EMAIL_SECONDARY AS (
        SELECT
            AGENTCODE,
            EMAILADDRESS,
            CASLAPPROVED
        FROM
            report.prod_insurance.brokeremail_vc
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
            report.prod_insurance.broker_vc B
            JOIN report.prod_insurance.brokeradvanced_vc BA ON B.AGENTCODE = BA.AGENTCODE
            LEFT JOIN EMAIL ON EMAIL.AGENTCODE = B.AGENTCODE --WHERE b.AGENTSTATUS in ('Active', 'Pending')
    ),
    --select * from AGT_LIST;
    
    SEGMENT_TAGS AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            report.prod_insurance.brokertags_vc
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
            report.prod_insurance.brokertags_vc
        WHERE
            TAGNAME = 'Elevated/élevée'
    ),
    PENDTERM AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            report.prod_insurance.brokertags_vc
        WHERE
            TAGNAME = 'Pending Termination/En attente de résiliation'
    ),
    TERM AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            report.prod_insurance.brokertags_vc
        WHERE
            TAGNAME = 'Terminated/Terminé'
    ),
    TFEROUT AS (
        SELECT
            AGENTCODE,
            TAGNAME
        FROM
            report.prod_insurance.brokertags_vc
        WHERE
            TAGNAME = 'Transferring Out/Transfert Sortant'
    ),
    COS_RSC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            report.prod_insurance.brokercos_vc COS
            LEFT JOIN report.prod_insurance.employee_vc EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Sales Coordinator'
    ),
    COS_BDC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            report.prod_insurance.brokercos_vc COS
            LEFT JOIN report.prod_insurance.employee_vc EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Business Development Consultant'
    ),
    COS_BDC_MAP AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            report.prod_insurance.brokercos_vc COS
            LEFT JOIN report.prod_insurance.employee_vc EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Business Development Consultant - MAP'
    ),
    COS_SD AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FULLNAME
        FROM
            report.prod_insurance.brokercos_vc COS
            LEFT JOIN report.prod_insurance.employee_vc EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Sales Director'
    ),
    COS_BOC AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Branch Office Coordinator'
    ),
    COS_ROM AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Region Operations Manager'
    ),
    COS_NBS_INF AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'New Business Specialist - Inforce'
    ),
    COS_NBS_CASEMGR AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        where
            ROLE = 'New Business Specialist - Case Manager'
    ),
    COS_NBS_INV AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'New Business Specialist - Investments'
    ),
    COS_INS_STRAT AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Insurance Strategist'
    ),
    COS_R_PRES_SALES AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional President - Sales'
    ),
    COS_CONTRACT_COORD AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Contracting Coordinator'
    ),
    COS_RVP_SALES AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Vice President - Sales'
    ),
    COS_CONTRACT_SPECIAL AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Contracting Specialist'
    ),
    COS_REG_COMPL_MGR AS (
        SELECT
            *,
            CONCAT(EE.FIRSTNAME, ' ', EE.LASTNAME) AS FullName
        FROM
            CLEAN.PROD_INSURANCE.BROKERCOS_VC COS
            LEFT JOIN CLEAN.PROD_INSURANCE.EMPLOYEE_VC EE ON EE.EMPLOYEECODE = COS.EMPLOYEECODE
        WHERE
            ROLE = 'Regional Compliance Manager'
    ),
    CONSOLIDATED_SEGMENT AS (
        SELECT
            SEG.*,
            CASE
                WHEN SEG.ELITE_AGT IS NOT NULL THEN 'Elite'
                WHEN SEG.MAP_ELITE IS NOT NULL THEN 'MAP-Elite'
                WHEN SEG.SELECT_AGT IS NOT NULL THEN 'Select'
                WHEN SEG.MAP_SELECT IS NOT NULL THEN 'MAP-Select'
                WHEN SEG.SIGNATURE_AGT IS NOT NULL THEN 'Signature'
                WHEN SEG.MAP_SIGNATURE IS NOT NULL THEN 'MAP-Signature'
                WHEN SEG.Advisor IS NOT NULL THEN 'Advisor'
                WHEN SEG.MAP_Advisor IS NOT NULL THEN 'MAP-Advisor'
                ELSE NULL
            END AS SEGMENTTAGWS,
            CASE
                WHEN ELEVATE.TAGNAME = 'Elevated/élevée' THEN 'Elevated'
            END AS ELEVATED,
            CASE
                WHEN PENDTERM.TAGNAME = 'Pending Termination/En attente de résiliation' THEN 'Pending Termination'
            END AS "PENDING TERMINATION",
            CASE
                WHEN TERM.TAGNAME = 'Terminated/Terminé' THEN 'Terminated'
            END AS TERMINATED,
            CASE
                WHEN TFEROUT.TAGNAME = 'Transferring Out/Transfert Sortant' THEN 'Transferring Out'
            END AS "TRANSFERRING OUT",
            CASE
                WHEN RSC.ROLE = 'Regional Sales Coordinator' THEN RSC.FULLNAME
            END AS RSC,
            CASE
                WHEN BDC.ROLE = 'Business Development Consultant' THEN BDC.FULLNAME
            END AS BDC,
            CASE
                WHEN MAP.ROLE = 'Business Development Consultant - MAP' THEN MAP.FULLNAME
            END AS BDC_MAP,
            CASE
                WHEN SD.ROLE = 'Sales Director' THEN SD.FULLNAME
            END AS SD,
            CASE
                WHEN BOC.ROLE = 'Branch Office Coordinator' THEN BOC.FULLNAME
            END AS BOC,
            CASE
                WHEN ROM.ROLE = 'Region Operations Manager' THEN ROM.FULLNAME
            END AS ROM,
            CASE
                WHEN INF.ROLE = 'New Business Specialist - Inforce' THEN INF.FULLNAME
            END AS NBS_INF,
            CASE
                WHEN CASEMGR.ROLE = 'New Business Specialist - Case Manager' THEN CASEMGR.FULLNAME
            END AS NBS_CMGR,
            CASE
                WHEN INV.ROLE = 'New Business Specialist - Investments' THEN INV.FULLNAME
            END AS NBS_INV,
            CASE
                WHEN INS_STRAT.ROLE = 'Insurance Strategist' THEN INS_STRAT.FULLNAME
            END AS INS_STRAT,
            CASE
                WHEN R_PRES_SALES.ROLE = 'Regional President - Sales' THEN R_PRES_SALES.FULLNAME
            END AS R_PRES,
            CASE
                WHEN CONTRACT_COORD.ROLE = 'Contracting Coordinator' THEN CONTRACT_COORD.FULLNAME
            END AS CONTRACT_COORD,
            CASE
                WHEN RVP_SALES.ROLE = 'Regional Vice President - Sales' THEN RVP_SALES.FULLNAME
            END AS RVP_SALES,
            CASE
                WHEN CONTRACT_SPECIAL.ROLE = 'Contracting Specialist' THEN CONTRACT_SPECIAL.FULLNAME
            END AS CONTRACT_SPECIAL,
            CASE
                WHEN REG_COMPL_MGR.ROLE = 'Regional Compliance Manager' THEN REG_COMPL_MGR.FULLNAME
            END AS REG_COMPL_MGR
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
            LEFT JOIN COS_BOC BOC ON BOC.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_ROM ROM ON ROM.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_NBS_INF INF ON INF.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_NBS_CASEMGR CASEMGR ON CASEMGR.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_NBS_INV INV ON INV.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_INS_STRAT INS_STRAT ON INS_STRAT.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_R_PRES_SALES R_PRES_SALES ON R_PRES_SALES.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_CONTRACT_COORD CONTRACT_COORD ON CONTRACT_COORD.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_RVP_SALES RVP_SALES ON RVP_SALES.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_CONTRACT_SPECIAL CONTRACT_SPECIAL ON CONTRACT_SPECIAL.AGENTCODE = SEG.AGENTCODE
            LEFT JOIN COS_REG_COMPL_MGR REG_COMPL_MGR ON REG_COMPL_MGR.AGENTCODE = SEG.AGENTCODE
    )
    SELECT
        *
    FROM
        CONSOLIDATED_SEGMENT
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48
