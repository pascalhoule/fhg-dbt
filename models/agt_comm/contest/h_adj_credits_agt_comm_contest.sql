{{ config( alias='h_adj_credits', database='agt_comm', schema='contest', materialized = "view" ) }} 

SELECT
    COALESCE(C.UD2, A.UD2) AS UD2,
    COALESCE(
        A.FYC_AMOUNT + C.FYC_AMOUNT,
        C.FYC_AMOUNT,
        A.FYC_AMOUNT
    ) AS FYC_AMOUNT,
    COALESCE(
        A.FYC_CREDITS + C.FYC_CREDITS,
        C.FYC_CREDITS,
        A.FYC_CREDITS
    ) AS FYC_CREDITS,
    COALESCE(
        A.SEG_SALES_AMOUNT + C.SEG_SALES_AMOUNT,
        C.SEG_SALES_AMOUNT,
        A.SEG_SALES_AMOUNT
    ) AS SEG_SALES_AMOUNT,
    COALESCE(
        A.SEG_SALES_CREDIT + C.SEG_SALES_CREDIT,
        C.SEG_SALES_CREDIT,
        A.SEG_SALES_CREDIT
    ) AS SEG_SALES_CREDIT,
    COALESCE(
        A.SEGAUMAMOUNT + C.SEGAUMAMOUNT,
        C.SEGAUMAMOUNT,
        A.SEGAUMAMOUNT
    ) AS SEGAUMAMOUNT,
    COALESCE(
        A.SEGAUMCREDIT + C.SEGAUMCREDIT,
        C.SEGAUMCREDIT,
        A.SEGAUMCREDIT
    ) AS SEGAUMCREDIT,
    COALESCE(
        A.MF_AUMAMOUNT + C.MF_AUMAMOUNT,
        C.MF_AUMAMOUNT,
        A.MF_AUMAMOUNT
    ) AS MF_AUMAMOUNT,
    COALESCE(
        A.MF_AUMCREDITS + C.MF_AUMCREDITS,
        C.MF_AUMCREDITS,
        A.MF_AUMCREDITS
    ) AS MF_AUMCREDITS,
    COALESCE(A.MF_SALES + C.MF_SALES, C.MF_SALES, A.MF_SALES) AS MF_SALES,
    COALESCE(
        A.MF_SALES_CREDITS + C.MF_SALES_CREDITS,
        C.MF_SALES_CREDITS,
        A.MF_SALES_CREDITS
    ) AS MF_SALES_CREDITS,
    A.MTH AS MTH,
    A.YR AS YR
FROM
    {{ ref('g_all_credits_agt_comm_contest') }} C FULL
    OUTER JOIN {{ ref('__base_adj_agt_comm_contest') }} A ON C.UD2 = A.UD2