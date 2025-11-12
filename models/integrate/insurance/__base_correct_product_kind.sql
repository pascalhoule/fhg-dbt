{{
    config(
        materialized="view",
        alias="__base_correct_product_kind",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]}
    )
}}

WITH MULT_PRODUCT_KIND AS (
    SELECT
        CURRENT_CONTRACT_POLICY_NUMBER,
        COUNT(DISTINCT PRODUCT_KIND) AS COUNT_OF_PRODUCT_KINDS
    FROM
        {{ source('acdirect_sandbox', 'daily_insurance_ac_direct_agreement') }} 
    GROUP BY
        1
    HAVING
        COUNT_OF_PRODUCT_KINDS > 1
    ORDER BY
        2 DESC
),
WITH_R AS (
    SELECT
        AC_DIRECT.CURRENT_CONTRACT_POLICY_NUMBER,
        AC_DIRECT.PRODUCT_KIND,
        AC_DIRECT.PRODUCT_TYPE,
        RANK () OVER (
            PARTITION BY AC_DIRECT.CURRENT_CONTRACT_POLICY_NUMBER
            ORDER BY
                AC_DIRECT.CURRENT_CONTRACT_POLICY_NUMBER,
                AC_DIRECT.PRODUCT_KIND
        ) AS R
    FROM
        {{ source('acdirect_sandbox', 'daily_insurance_ac_direct_agreement') }} AS AC_DIRECT
        JOIN MULT_PRODUCT_KIND ON MULT_PRODUCT_KIND.CURRENT_CONTRACT_POLICY_NUMBER = AC_DIRECT.CURRENT_CONTRACT_POLICY_NUMBER
    ORDER BY
        AC_DIRECT.CURRENT_CONTRACT_POLICY_NUMBER,
        AC_DIRECT.PRODUCT_KIND
),
WITH_R_DEDUP AS (
    SELECT
        DISTINCT CURRENT_CONTRACT_POLICY_NUMBER,
        PRODUCT_KIND,
        R
    FROM
        WITH_R
    ORDER BY
        1, 3
),
CORRECTIONS AS (
    SELECT
        CURRENT_CONTRACT_POLICY_NUMBER,
        PRODUCT_KIND,
        R,
        FIRST_VALUE(PRODUCT_KIND) respect nulls OVER (
            PARTITION BY CURRENT_CONTRACT_POLICY_NUMBER
            ORDER BY
                R ASC
        ) AS FIRST_VAL,
        NTH_VALUE(PRODUCT_KIND, 2)
    FROM
        FIRST RESPECT NULLS OVER (
            PARTITION BY CURRENT_CONTRACT_POLICY_NUMBER
            ORDER BY
                R ASc
        ) AS SECOND_VAL,
        CASE
            WHEN RIGHT(CURRENT_CONTRACT_POLICY_NUMBER, 1) = 'U' THEN 'Universal Life'
            WHEN RIGHT(CURRENT_CONTRACT_POLICY_NUMBER, 1) = 'P' THEN 'Par'
            WHEN RIGHT(CURRENT_CONTRACT_POLICY_NUMBER, 1) = 'D' THEN 'Disability'
            WHEN RIGHT(CURRENT_CONTRACT_POLICY_NUMBER, 1) = 'T' THEN 'Term'
            WHEN RIGHT(CURRENT_CONTRACT_POLICY_NUMBER, 1) = 'C' THEN 'Critical Illness'
            WHEN FIRST_VAL = 'Critical Illness'
            AND SECOND_VAL = 'Disability' THEN 'Disability'
            WHEN FIRST_VAL = 'Par'
            AND SECOND_VAL = 'Universal Life' THEN 'Universal Life' --might need to be corrected.
            WHEN FIRST_VAL = 'Par'
            AND SECOND_VAL = 'Term' THEN 'Par' --might need to be corrected.
            WHEN FIRST_VAL = 'Universal Life'
            AND SECOND_VAL = 'Term' THEN 'Universal Life' --might need to be corrected.
            WHEN FIRST_VAL = 'Par'
            AND SECOND_VAL = 'Perm' THEN 'Permanent'
            WHEN FIRST_VAL = 'Perm'
            AND SECOND_VAL = 'Term' THEN 'Permanent'
            WHEN FIRST_VAL = 'Term'
            AND SECOND_VAL = 'Universal Life' THEN 'Universal Life' --might need to be corrected.
            ELSE 'Unknown'
        END AS REVISED_PRODUCT_KIND
    FROM
        WITH_R_DEDUP
)
SELECT
    DISTINCT CURRENT_CONTRACT_POLICY_NUMBER,
    REVISED_PRODUCT_KIND
FROM
    CORRECTIONS