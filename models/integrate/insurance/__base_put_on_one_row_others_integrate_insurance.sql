{{
    config(
        materialized="table",
        alias="__base_put_on_one_row_others",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]}
    )
}}

WITH POL_LIST AS (
    SELECT
        CURRENT_CONTRACT_POLICY_NUMBER,
        ADVISOR_AGREEMENT_GROUP_IDENTIFIER,
        PRODUCT_KIND,
        SUM(PLACED_TOTAL_SALES_MEASURE) AS PLACED_TOTAL_SALES_MEASURE,
        SUM(FYC_AMOUNT) AS FYC_AMOUNT
    FROM
        {{ ref('__base_put_on_one_row_disability_integrate_insurance') }}
    GROUP BY
        1,2,3
    HAVING
        SUM(PLACED_TOTAL_SALES_MEASURE) > 0
        AND SUM(FYC_AMOUNT) > 0
),
CASES_TO_CORRECT AS (
    SELECT
        CURRENT_CONTRACT_POLICY_NUMBER,
        ADVISOR_AGREEMENT_GROUP_IDENTIFIER,
        PRODUCT_KIND,
        PRODUCT_TYPE
    FROM
        {{ ref('__base_put_on_one_row_disability_integrate_insurance') }}
    WHERE
        CURRENT_CONTRACT_POLICY_NUMBER IN (
            SELECT
                DISTINCT CURRENT_CONTRACT_POLICY_NUMBER
            FROM
                POL_LIST
        )
        AND FYC_AMOUNT = 0
        AND PLACED_TOTAL_SALES_MEASURE > 0
    GROUP BY
        ALL
),
CORRECT_METRICS AS (
        SELECT
            C.CURRENT_CONTRACT_POLICY_NUMBER,
            C.ADVISOR_AGREEMENT_GROUP_IDENTIFIER,
            C.PRODUCT_KIND,
            C.product_type,
            SUM(PLACED_TOTAL_SALES_MEASURE) AS PLACED_TOTAL_SALES_MEASURE,
            SUM(PENDING_DECIDED_TOTAL_SALES_MEASURE) AS PENDING_DECIDED_TOTAL_SALES_MEASURE,
            MAX(SALES_COUNT_CREDIT) AS SALES_COUNT_CREDIT,
            MAX(PLACED_POLICY_COUNT) AS PLACED_POLICY_COUNT,
            MAX(PENDING_POLICY_COUNT) AS PENDING_POLICY_COUNT,
            SUM(FYC_AMOUNT) AS FYC_AMOUNT
        FROM
            CASES_TO_CORRECT C
            LEFT JOIN INTEGRATE.PROD_INSURANCE.__BASE_PUT_ON_ONE_ROW_DISABILITY D on C.CURRENT_CONTRACT_POLICY_NUMBER = D.CURRENT_CONTRACT_POLICY_NUMBER
            AND C.ADVISOR_AGREEMENT_GROUP_IDENTIFIER = D.ADVISOR_AGREEMENT_GROUP_IDENTIFIER
            AND C.PRODUCT_KIND = d.PRODUCT_KIND
        GROUP BY
            1,2,3,4
    ),
    CORRECT_METRICS_DROP_PRODUCT_TYPE AS (
        SELECT
            CURRENT_CONTRACT_POLICY_NUMBER,
            ADVISOR_AGREEMENT_GROUP_IDENTIFIER,
            PRODUCT_KIND,
            PLACED_TOTAL_SALES_MEASURE,
            PENDING_DECIDED_TOTAL_SALES_MEASURE,
            SALES_COUNT_CREDIT,
            PLACED_POLICY_COUNT,
            PENDING_POLICY_COUNT,
            FYC_AMOUNT
        FROM
            CORRECT_METRICS
        GROUP BY
            ALL
    ),
    RECS_UNCHANGED AS (
        SELECT
            D.*
        FROM
            {{ ref('__base_put_on_one_row_disability_integrate_insurance') }} D
            LEFT JOIN CORRECT_METRICS_DROP_PRODUCT_TYPE C on C.CURRENT_CONTRACT_POLICY_NUMBER = D.CURRENT_CONTRACT_POLICY_NUMBER
            AND C.ADVISOR_AGREEMENT_GROUP_IDENTIFIER = D.ADVISOR_AGREEMENT_GROUP_IDENTIFIER
            AND C.PRODUCT_KIND = D.PRODUCT_KIND
        WHERE
            C.CURRENT_CONTRACT_POLICY_NUMBER IS NULL
    ),
    CORRECTED_RECORDS AS (
        SELECT
            C.ADVISOR_AGREEMENT_GROUP_IDENTIFIER,
            FINANCIAL_HORIZONS_GROUP_IDENTIFIER,
            ADVISOR_PRODUCER_NAME,
            ADVISOR_AGREEMENT_NUMBER,
            ADVISOR_REPORTING_FIRM_NAME,
            AC_REGION,
            AC_MARKET,
            AC_LOCATION,
            C.PRODUCT_KIND,
            '-' AS PRODUCT_TYPE,
            C.CURRENT_CONTRACT_POLICY_NUMBER,
            CARRIER,
            CURRENT_POLICY_STATUS,
            SETTLEMENT_DATE,
            APPLICATION_DATE,
            FIRST_COMMISSION_DATE,
            C.PLACED_TOTAL_SALES_MEASURE,
            C.PENDING_DECIDED_TOTAL_SALES_MEASURE,
            C.SALES_COUNT_CREDIT,
            C.PLACED_POLICY_COUNT,
            C.PENDING_POLICY_COUNT,
            C.FYC_AMOUNT
        FROM
            {{ ref('__base_put_on_one_row_disability_integrate_insurance') }} D
            JOIN CORRECT_METRICS C ON C.CURRENT_CONTRACT_POLICY_NUMBER = D.CURRENT_CONTRACT_POLICY_NUMBER
            AND C.ADVISOR_AGREEMENT_GROUP_IDENTIFIER = D.ADVISOR_AGREEMENT_GROUP_IDENTIFIER
            AND C.PRODUCT_KIND = D.PRODUCT_KIND
        GROUP BY
            ALL
    )
SELECT
    *
FROM
    CORRECTED_RECORDS
UNION
SELECT
    *
FROM
    RECS_UNCHANGED