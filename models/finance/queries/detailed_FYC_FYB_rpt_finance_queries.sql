{{  config(alias='detailed_FYC_FYB_rpt', 
    database='finance', 
    schema='queries',
    materialization = "view")  }} 

SELECT
    ACCURALCODE AS ACCRUALCODE,
    COMMISSION_RUN AS "Commission Run",
    OCCURRENCE_TYPE AS "Occurrence Type",
    WRITING_AGENT AS "Writing Agent",
    REP_ID AS "Rep ID",
    OWNER_TYPE AS "Owner Type",
    OWNER_NAME AS "Owner Name",
    OWNER_COMPANY AS "Owner Company",
    POLICY_NUMBER AS "Policy Number",
    POLICYCODE AS "Coverage Number",
    CARRIER AS "Carrier",
    CASE
        WHEN PLAN_NAME = '-Bulk Travel' THEN 'Bulk Travel'
        ELSE PLAN_NAME
    END AS "Plan Name",
    A.PLANCATEGORY AS "Plan Category",
    STATUS AS "Status",
    TOTAL_FYC AS "Total FYC",
    TOTAL_FYB AS "Total FYB",
    POSTDATE AS "Post Date",
    TRANSACTION_COMMISSIONABLE_PREMIUM AS "Transaction Commissionable Premium",
    COMMISSION_RATE AS "Commission Rate",
    COMMISSION_SPLIT_SHARE AS "Commission Split Share",
FROM
    {{ ref('detailed_FYC_FYB_finance_queries') }} A
    JOIN {{ ref('plancategory_map_finance_queries') }} B ON A.PLANCATEGORY = B.PLANCATEGORY