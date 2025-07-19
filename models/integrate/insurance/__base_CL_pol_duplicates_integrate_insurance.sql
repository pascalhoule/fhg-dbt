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
<<<<<<< HEAD
FROM {{ source("acdirect_leo", "daily_insurance_ac_direct_agreement") }} AS CL
=======
FROM {{ source("acdirect_policy", "daily_insurance_AC_Direct_agreement_20250716") }} AS CL
>>>>>>> db0943df7d23027ef14622120bd157bb9ababcb0
inner join
    {{ ref('policy_fh_integrate_insurance') }} AS FH
    on CL.CURRENT_CONTRACT_POLICY_NUMBER = FH.POLICYNUMBER
