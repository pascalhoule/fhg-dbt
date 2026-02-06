{{			
    config (			
        materialized="view",			
        alias='mth_seg_production', 			
        database='report', 			
        schema='segfund',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

WITH RPT AS (
    SELECT
        YEAR(SEGFUND_REPORT_DATE_FH) AS YR,
        MONTH(SEGFUND_REPORT_DATE_FH) AS MTH,
        FINANCE_CATEGORY_FH,
        MAP.SUB_FINANCE_CATEGORY,
        T.FUNDPRODUCTCODE,
        T.FUNDACCOUNT_CODE,
        REP.REPID,
        REP.INSAGENTCODE,
        REP.REPRESENTATIVECODE,
        CONCAT(REP.LAST_NAME, ',', REP.FIRST_NAME) AS FULLNAME,
        REP.LAST_NAME,
        REP.FIRST_NAME,
        FUNDPRODUCT.SPONSORID,
        FUNDPRODUCT.FUNDID,
        FUNDPRODUCT.NAME AS FUNDNAME,
        FUNDPRODUCT.LOADTYPE,
        FUNDPRODUCT.SUBTYPENAME,
        FUNDPRODUCT.SERVICEFEERATE,
        SUM(T.AMOUNT) AS SEG_SALES_AMT,
        SUM(T.GROSSCOMMISSION) AS GROSSCOMMISSION
    FROM
        {{ ref('segfund_report_transactions_fh_report_investment') }} T
        JOIN {{ ref('representatives_vc_report_investment') }} REP ON REP.REPRESENTATIVECODE = T.TRANSACTIONREPCODE
        JOIN {{ ref('transact_map_report_segfund') }} MAP ON T.TRANSACTIONTYPECODE = MAP.TRANSACTIONTYPECODE
        JOIN {{ ref('fundproducts_vc_report_investment') }} FUNDPRODUCT ON T.FUNDPRODUCTCODE = FUNDPRODUCT.FUNDPRODUCTCODE
        JOIN {{ ref('sponsor_vc_report_investment') }} SPONSOR ON SPONSOR.SPONSORID = FUNDPRODUCT.SPONSORID
    WHERE
        YEAR(SEGFUND_REPORT_DATE_FH) >= YEAR(CURRENT_DATE) - 1
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    HAVING
        FINANCE_CATEGORY_FH in ('DEPOSIT')
    UNION
    SELECT
        YEAR(SEGFUND_REPORT_DATE_FH) AS YR,
        MONTH(SEGFUND_REPORT_DATE_FH) AS MTH,
        FINANCE_CATEGORY_FH,
        MAP.SUB_FINANCE_CATEGORY,
        T.FUNDPRODUCTCODE,
        T.FUNDACCOUNT_CODE,
        REP.REPID,
        REP.INSAGENTCODE,
        REP.REPRESENTATIVECODE,
        CONCAT(REP.LAST_NAME, ',', REP.FIRST_NAME) AS FULLNAME,
        REP.LAST_NAME,
        REP.FIRST_NAME,
        FUNDPRODUCT.SPONSORID,
        FUNDPRODUCT.FUNDID,
        FUNDPRODUCT.NAME AS FUNDNAME,
        FUNDPRODUCT.LOADTYPE,
        FUNDPRODUCT.SUBTYPENAME,
        FUNDPRODUCT.SERVICEFEERATE,
        SUM(T.NETAMOUNT) AS SEG_SALES_AMT,
        SUM(T.GROSSCOMMISSION) AS GROSSCOMMISSION
    FROM
        {{ ref('segfund_report_transactions_fh_report_investment') }}  T
        JOIN {{ ref('representatives_vc_report_investment') }} REP ON REP.REPRESENTATIVECODE = T.TRANSACTIONREPCODE
        JOIN {{ ref('transact_map_report_segfund') }} MAP ON T.TRANSACTIONTYPECODE = MAP.TRANSACTIONTYPECODE
        JOIN {{ ref('fundproducts_vc_report_investment') }}  FUNDPRODUCT ON T.FUNDPRODUCTCODE = FUNDPRODUCT.FUNDPRODUCTCODE
        JOIN {{ ref('sponsor_vc_report_investment') }} SPONSOR ON SPONSOR.SPONSORID = FUNDPRODUCT.SPONSORID
    WHERE
        YEAR(SEGFUND_REPORT_DATE_FH) >= YEAR(CURRENT_DATE) - 1
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    HAVING
        FINANCE_CATEGORY_FH = 'WITHDRAWAL'
),
CARRIER_MAP AS (
    SELECT
        DISTINCT SPONSOR_ID,
        CARRIER_DATABASE_NAME
    FROM
        {{ ref('carrier_fin_fh_report_investment') }}
),
COS_SEGMENT_MAP AS (
    SELECT
        REPMAP.*,
        SEGMENTTAGWS AS SEGMENT,
        COS_SALES_RSC,
        COS_SALES_BDC,
        COS_SALES_BDD,
        COS_SALES_SVP,
        COS_SALES_RVP,
        COS_SALES_VP,
        COS_CONTRACT_CC,
        COS_CONTRACT_CS,
        COS_COMPLIANCE_RCM,
        COS_OPS_BOC,
        COS_OPS_NBS_INV,
        COS_OPS_NBS_CM,
        COS_OPS_NBS_INF,
        COS_OPS_ROM,
        COS_RAM,
        COS_SALESIS AS COS_SALES_IS,
        COS_SALES_WS,
        COS_CONTRACT_RMCC
    FROM
        {{ ref('repmap_fh_report_investment') }} REPMAP
        LEFT JOIN {{ ref('broker_fh_report_insurance') }} B ON REPMAP.REPID = B.BROKERID
    WHERE
        REPID IS NOT NULL
    GROUP BY
        ALL
)
SELECT
    RPT.*,
    CARRIER_DATABASE_NAME AS CARRIER,
    SEGMENT,
    COS_SEGMENT_MAP.BRANCH_FH, 
    COS_SEGMENT_MAP.REGION_FH,
    COS_SALES_RSC,
    COS_SALES_BDC,
    COS_SALES_BDD,
    COS_SALES_SVP,
    COS_SALES_RVP,
    COS_SALES_VP,
    COS_CONTRACT_CC,
    COS_CONTRACT_CS,
    COS_COMPLIANCE_RCM,
    COS_OPS_BOC,
    COS_OPS_NBS_INV,
    COS_OPS_NBS_CM,
    COS_OPS_NBS_INF,
    COS_OPS_ROM,
    COS_RAM,
    COS_SALES_IS,
    COS_SALES_WS,
    COS_CONTRACT_RMCC
FROM
    RPT
    LEFT JOIN COS_SEGMENT_MAP ON RPT.REPID = COS_SEGMENT_MAP.REPID
    AND RPT.REPRESENTATIVECODE = COS_SEGMENT_MAP.REPRESENTATIVECODE
    LEFT JOIN CARRIER_MAP C ON C.SPONSOR_ID = RPT.SPONSORID
