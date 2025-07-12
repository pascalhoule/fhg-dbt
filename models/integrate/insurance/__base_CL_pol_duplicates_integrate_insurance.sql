{{
    config(
        materialized="view",
        alias="__base_CL_pol_duplicates",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]},
    )
}}

SELECT DISTINCT
    FH.POLICYNUMBER,
    CL.CURRENT_CONTRACT_POLICY_NUMBER
from {{ source("acdirect", "daily_insurance_ac_direct_agreement") }} as CL
inner join
    {{ ref('policy_fh_integrate_insurance') }} as FH
    on CL.CURRENT_CONTRACT_POLICY_NUMBER = FH.POLICYNUMBER
