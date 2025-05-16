{{ config( 
    alias='g_all_credits', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

WITH LIFE_AND_SEG AS (
    SELECT
        COALESCE(LIFE.UD2, SEG.UD2) AS UD2,
        COALESCE(LIFE.AGENTCODE, SEG.AGENTCODE) AS AGENTCODE,
        SUM(LIFE.FYC_AMOUNT) AS FYC_AMOUNT,
        SUM(LIFE.FYC_CREDITS) AS FYC_CREDITS,
        SUM(SEG.SEG_SALES_AMOUNT) AS SEG_SALES_AMOUNT,
        SUM(SEG.SEG_SALES_CREDIT) AS SEG_SALES_CREDIT,
    FROM
        {{ ref('b_life_credits_agt_comm_contest') }} LIFE FULL
        OUTER JOIN {{ ref('c_segfund_credits_agt_comm_contest') }} SEG ON LIFE.UD2 = SEG.UD2
        AND LIFE.AGENTCODE = SEG.AGENTCODE
    GROUP BY
        1, 2
),
AUM_AND_LIFE_AND_SEG AS (
    SELECT
        COALESCE(AUM.UD2, LIFE_AND_SEG.UD2) AS UD2,
        SUM(FYC_AMOUNT) AS FYC_AMOUNT,
        SUM(FYC_CREDITS) AS FYC_CREDITS,
        MIN(SEG_SALES_AMOUNT) AS SEG_SALES_AMOUNT,
        MIN(SEG_SALES_CREDIT) AS SEG_SALES_CREDIT,
        SUM(SEG_AUMAMOUNT) AS SEG_AUMAMOUNT,
        SUM(SEG_AUMCREDIT) AS SEG_AUMCREDIT
    FROM
        {{ ref('d_aum_credits_agt_comm_contest') }} AUM FULL
        OUTER JOIN LIFE_AND_SEG ON AUM.UD2 = LIFE_AND_SEG.UD2
        AND AUM.AGENTCODE = LIFE_AND_SEG.AGENTCODE
    WHERE
        NOT (
            FYC_CREDITS IS NULL
            AND SEG_SALES_CREDIT IS NULL
            AND SEG_AUMCREDIT = 0
        )
    GROUP BY
        1 
)
SELECT
    UD2,
    SUM(FYC_AMOUNT) AS FYC_AMOUNT,
    SUM(SEG_SALES_AMOUNT) AS SEG_SALES_AMOUNT,
    SUM(SEG_AUMAMOUNT) AS SEGAUMAMOUNT,
FROM
    AUM_AND_LIFE_AND_SEG
WHERE
    UD2 is not null
    and contains(UD2, '/') = false
    and contains(UD2, '-') = false
GROUP BY
    1   