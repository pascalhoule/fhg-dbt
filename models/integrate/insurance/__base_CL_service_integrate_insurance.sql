{{
    config(
        materialized="view",
        alias="__base_cl_service",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]}
    )
}}


SELECT
    'SERVICE' AS FH_POLICYCATEGORY,
    CURRENT_CONTRACT_POLICY_NUMBER,
    CURRENT_POLICY_STATUS,
    APPLICATION_DATE,
    ROUND(sum(FYC_AMOUNT), 2) AS FYC_AMOUNT,
    sum(PLACED_TOTAL_SALES_MEASURE) AS PLACED_TOTAL_SALES_MEASURE,
    sum(SALES_COUNT_CREDIT) AS SALES_COUNT_CREDIT
FROM
    {{ ref('__base_CL_Data_integrate_insurance') }}
WHERE CURRENT_POLICY_STATUS = 'Placed'
GROUP BY 1, 2, 3, 4
HAVING sum(FYC_AMOUNT) = 0
