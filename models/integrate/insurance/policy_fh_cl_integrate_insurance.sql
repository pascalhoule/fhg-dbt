{{
    config(
        materialized="view",
        alias="policy_fh_cl",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]},
    )
}}



WITH unioned_data AS (

select
    fh_policycategory,
    policycode,
    cast(policygroupcode as VARCHAR(50)) as policygroupcode,
    cast (fh_commissioningagtcode as VARCHAR(50)) as fh_servicingagtcode,
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
    try_cast(advisor_agreement_group_identifier as varchar(50)) as fh_servicingagtcode,
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
    WHEN product_kind = 'Permanent' THEN 'Permanent'
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
    COALESCE(
    CASE 
        WHEN current_policy_status IN ('Pending', 'Decided') 
        THEN pending_policy_count 
        ELSE 0
    END, 0
        ) +
    COALESCE(
    CASE 
        WHEN current_policy_status NOT IN ('Pending', 'Decided') 
        THEN placed_policy_count
        ELSE 0
    END, 0
        ) AS appcount,
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
    try_cast (SETTLEMENT_DATE as Date) as FH_SETTLEMENTDATE,
    try_cast (application_date as date) as	FH_STARTDATE,
    null as	FH_PREMIUM,
    COALESCE(
    CASE 
        WHEN current_policy_status IN ('Pending', 'Decided') 
        THEN TRY_CAST(REPLACE(REPLACE(pending_decided_total_sales_measure, '$',''),',','') AS FLOAT)
        ELSE 0
    END, 0
        ) +
    COALESCE(
    CASE 
        WHEN current_policy_status NOT IN ('Pending', 'Decided') 
        THEN TRY_CAST(REPLACE(REPLACE(placed_total_sales_measure, '$',''),',','') AS FLOAT)
        ELSE 0
    END, 0
        ) AS FH_PREM_SERVWGT,
    CASE 
    WHEN current_policy_status NOT IN ('Pending', 'Decided') 
    THEN TRY_CAST(REPLACE(REPLACE(placed_total_sales_measure, '$',''),',','') AS FLOAT)
    ELSE 0
END AS FH_PREM_COMMWGT	,
null as APPLICATIONDATE,
try_cast (first_commission_date as date) as FH_PLACEDDATE,
null as	FH_FYCPLACED

from
{{ source("acdirect", "daily_insurance_acdirect") }}
)
Select u.*, B.BrokerID,  
    H.REGION, 
    H.Market, 
    B.COS_SALES_BDC, 
    B.COS_SALES_BDD, 
    B.COS_SALES_RVP, 
    REGEXP_REPLACE(B.USERDEFINED2, '^FH', '') AS USERDEFINED2, 
    H.LOCATION, 
    B.SEGMENTTAGWS
from unioned_data u
left join {{ ref('broker_fh_cl_integrate_insurance') }} B
on u. FH_COMMISSIONINGAGTCODE = B.AGENTCODE
left join {{ ref('hierarchy_fh_cl_integrate_insurance') }} H
on u. FH_COMMISSIONINGAGTCODE = H.AGENTCODE


