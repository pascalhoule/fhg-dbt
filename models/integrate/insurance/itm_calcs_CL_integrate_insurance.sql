{{ config(alias = 'itm_calcs_CL', 
    materialized = 'view',
    database = 'normalize', 
    schema = 'insurance') }} 

WITH POL_GT_ONE_REC AS (
    SELECT
        CURRENT_CONTRACT_POLICY_NUMBER AS POL_NO,
        COUNT(*) REC_COUNT
    FROM
        {{ ref('daily_insurance_policy_cl_detail') }}
    GROUP BY
        1
    HAVING
        REC_COUNT > 1
),
POL_ONE_REC AS (
    SELECT
        CURRENT_CONTRACT_POLICY_NUMBER AS POL_NO,
        COUNT(*) REC_COUNT
    FROM
        {{ ref('daily_insurance_policy_cl_detail') }}
    GROUP BY
        1
    HAVING
        REC_COUNT = 1
),
FILTER_OUT AS (
    SELECT
        P.*
    FROM
        {{ ref('daily_insurance_policy_cl_detail') }} P
        LEFT JOIN POL_GT_ONE_REC ON POL_GT_ONE_REC.POL_NO = P.CURRENT_CONTRACT_POLICY_NUMBER
    WHERE
        NOT(
            FYC_AMOUNT = 0
            AND PENDING_POLICY_COUNT = 0
            AND PLACED_POLICY_COUNT = 0
            AND SALES_COUNT_CREDIT = 0
            AND PENDING_DECIDED_TOTAL_SALES_MEASURE = 0
            AND PLACED_TOTAL_SALES_MEASURE = 0
        )
        AND POL_GT_ONE_REC.POL_NO IS NOT NULL
),
DROP_ZERO_ROWS AS (
    SELECT
        *
    FROM
        FILTER_OUT
    UNION
    SELECT
        P.*
    FROM
        {{ ref('daily_insurance_policy_cl_detail') }} P
        JOIN POL_ONE_REC on POL_ONE_REC.POL_NO = P.CURRENT_CONTRACT_POLICY_NUMBER
)
SELECT
    CURRENT_CONTRACT_POLICY_NUMBER,
    COALESCE(FIRST_COMMISSION_DATE, SETTLEMENT_DATE) AS FH_SETTLEMENTDATE,
    CURRENT_POLICY_STATUS,
    SETTLEMENT_DATE,
    APPLICATION_DATE,
    FIRST_COMMISSION_DATE,
    COALESCE(FIRST_COMMISSION_DATE, SETTLEMENT_DATE) AS FH_ITM_END_DATE,
    CASE
        WHEN APPLICATION_DATE = '9999-12-31'
        OR FH_ITM_END_DATE = '9999-12-31' THEN 99999
        ELSE DATEDIFF('d', APPLICATION_DATE, FH_ITM_END_DATE)
    END AS FH_ITM,
    CASE
        WHEN FH_ITM_END_DATE = '9999-12-31'
        OR APPLICATION_DATE = '9999-12-31' THEN NULL
        ELSE DATEDIFF('d', APPLICATION_DATE, FH_ITM_END_DATE)
    END AS FH_DAYS_IN_STATUS
FROM
    DROP_ZERO_ROWS    