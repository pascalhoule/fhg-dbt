{{
    config(
        materialized="view",
        alias="policy_fh_cl_original",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]},
    )
}}


select
    fh_policycategory,
    policycode,
    cast(policygroupcode as VARCHAR(50)) as policygroupcode,
    fh_servicingagtcode,
    fh_servicingagtsplit,
    cast(fh_commissioningagtcode as VARCHAR(50)) as fh_commissioningagtcode,
    fh_commissioningagtsplit,
    fh_carriereng,
    fh_carrierfr,
    policynumber,
    planid,
    fh_plantype,
    fh_plannameeng,
    fh_plannamefr,
    premiumamount,
    annualpremiumamount,
    commpremiumamount,
    paymentmode,
    faceamount,
    createdby,
    createddate,
    contractdate,
    settlementdate,
    expirydate,
    renewaldate,
    senttoicdate,
    maileddate,
    fh_finpostdate,
    fh_statuscode,
    fh_statusnameeng,
    fh_statusnamefr,
    fh_statuscategory,
    appcount,
    fh_fycservamt,
    fh_fyccommamt,
    mgafyoamount,
    issueprovince,
    fh_appsource,
    fh_apptype,
    lastcommissionprocessdate,
    firstownerclientcode,
    firstinsuredclientcode,
    ismaincoverage,
    fh_settlementdate,
    fh_startdate,
    fh_premium,
    fh_prem_commwgt,
    fh_prem_servwgt,
    applicationdate,
    fh_placeddate,
    fh_fycplaced
from {{ ref("policy_fh_integrate_insurance") }}

union all


select
    'NEW POLICY' as fh_policycategory,
    null as policycode,
    current_contract_policy_number as policygroupcode,
    null as fh_servicingagtcode,
    null as fh_servicingagtsplit,
    try_cast(advisor_agreement_group_identifier as varchar(50)) as fh_commissioningagtcode,
    null as fh_commissioningagtsplit,
    'CL Direct' as fh_carriereng,
    'CL Direct' as fh_carrierfr,
    current_contract_policy_number as policynumber,
    null as planid,
    Case     
    WHEN product_kind = 'Critical Illness' THEN 'CI'
    WHEN product_kind = 'Disability' THEN 'DI'
    WHEN product_kind = 'Universal Life' THEN 'UL'
    WHEN product_kind = 'Perm' THEN 'Permanent'
    ELSE product_kind
END AS fh_plantype,
    product_type as fh_plannameeng,
    product_type as fh_plannamefr,
    null as premiumamount,
    null as annualpremiumamount,
    null as commpremiumamount,
    null as paymentmode,
    null as faceamount,
    null as createdby,
    null as createddate,
    null as contractdate,
    null as settlementdate,
    null as expirydate,
    null as renewaldate,
    null as senttoicdate,
    null as maileddate,
    null as fh_finpostdate,
    null as fh_statuscode,
    null as fh_statusnameeng,
    null as fh_statusnamefr,
    current_policy_status as fh_statuscategory,
    policy_count as appcount,
    null as fh_fycservamt,
    try_cast(replace(replace(fyc_amount, '$', ''), ',', '') as float) as fh_fyccommamt,
    null as mgafyoamount,
    null as issueprovince,
    null as fh_appsource,
    null as fh_apptype,
    null as lastcommissionprocessdate,
    null as firstownerclientcode,
    null as firstinsuredclientcode,
    null as ismaincoverage,
    try_cast(settlement_date as date) as fh_settlementdate,
    null as fh_startdate,
    null as fh_premium,
    try_cast(
        replace(replace(placed_total_sales_measure, '$', ''), ',', '') as float
    ) + try_cast(
        replace(replace(pending_decided_total_sales_measure, '$', ''), ',', '') as float
    ) as fh_prem_commwgt,
    null as fh_prem_servwgt,
    try_cast(application_date as date) as applicationdate,
    try_cast(first_commission_date as date) as fh_placeddate,
    null as fh_fycplaced
from {{ source("acdirect", "daily_insurance_acdirect") }}
