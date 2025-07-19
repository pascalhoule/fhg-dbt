{{
    config(
        materialized="view",
        alias="__base_CL_pol_duplicates",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]}
    )
}}


SELECT DISTINCT
    FH.POLICYNUMBER,
    CL.CURRENT_CONTRACT_POLICY_NUMBER
FROM {{ ref('daily_insurance_policy_cl_detail') }} AS CL
inner join
    {{ ref('policy_fh_integrate_insurance') }} AS FH
    on CL.CURRENT_CONTRACT_POLICY_NUMBER = FH.POLICYNUMBER
