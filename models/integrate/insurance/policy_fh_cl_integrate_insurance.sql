{{
    config(
        materialized="view",
        alias="policy_fh_cl",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]},
    )
}}

WITH

<<<<<<< HEAD
POL_WITH_UD2 AS (
    SELECT
        POL_BASE.*,
        B_SERV.USERDEFINED2 AS FH_SERVICINGAGT_UD2,
        B_COMM.USERDEFINED2 AS FH_COMMISSIONINGAGT_UD2
    FROM
        {{ ref('__base_pol_fh_cl_integrate_insurance') }} AS POL_BASE
    LEFT JOIN
        {{ ref('broker_fh_cl_integrate_insurance') }} AS B_COMM
        ON POL_BASE.FH_COMMISSIONINGAGTCODE = B_COMM.AGENTCODE
    LEFT JOIN
        {{ ref('broker_fh_cl_integrate_insurance') }} AS B_SERV
        ON POL_BASE.FH_SERVICINGAGTCODE = B_SERV.AGENTCODE
),
=======

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
>>>>>>> a8ace30bfa7db36b80ef8bf452f4b370dbc3f004

HIER_W_UD2 AS (
    SELECT
        H.*,
        REGEXP_REPLACE(B.USERDEFINED2, '^FH', '') AS UD2
    FROM {{ ref('broker_fh_cl_integrate_insurance') }} AS B
    INNER JOIN
        {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H
        ON B.AGENTCODE = H.AGENTCODE
),

BROKER_ACTIVE AS (
    SELECT DISTINCT
        B.USERDEFINED2,
        FIRST_VALUE(B.AGENTNAME)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTNAME,
        FIRST_VALUE(B.AGENTCODE)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTCODE,
        FIRST_VALUE(B.BROKERID)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS BROKERID,
        FIRST_VALUE(H.REGION)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS REGION,
        FIRST_VALUE(H.MARKET)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS MARKET,
        FIRST_VALUE(H.LOCATION)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS LOCATION,
        FIRST_VALUE(B.COS_SALES_BDC)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDC,
        FIRST_VALUE(B.COS_SALES_BDD)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDD,
        FIRST_VALUE(B.COS_SALES_RVP)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_RVP,
        FIRST_VALUE(B.SEGMENTTAGWS)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS SEGMENTTAGWS
    FROM {{ ref('broker_fh_cl_integrate_insurance') }} AS B
    INNER JOIN
        {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H
        ON B.AGENTCODE = H.AGENTCODE
    WHERE B.AGENTSTATUS = 'Active'
),


<<<<<<< HEAD
REST_OF_BROKER_LIST AS (
    SELECT DISTINCT
        B.USERDEFINED2,
        FIRST_VALUE(B.AGENTNAME)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTNAME,
        FIRST_VALUE(B.AGENTCODE)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTCODE,
        FIRST_VALUE(B.BROKERID)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS BROKERID,
        FIRST_VALUE(H.REGION)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS REGION,
        FIRST_VALUE(H.MARKET)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS MARKET,
        FIRST_VALUE(H.LOCATION)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS LOCATION,
        FIRST_VALUE(B.COS_SALES_BDC)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDC,
        FIRST_VALUE(B.COS_SALES_BDD)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDD,
        FIRST_VALUE(B.COS_SALES_RVP)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_RVP,
        FIRST_VALUE(B.SEGMENTTAGWS)
            OVER (
                PARTITION BY B.USERDEFINED2
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS SEGMENTTAGWS
    FROM {{ ref('broker_fh_cl_integrate_insurance') }} AS B
    INNER JOIN
        {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H
        ON B.AGENTCODE = H.AGENTCODE
    WHERE B.USERDEFINED2 NOT IN (SELECT B.USERDEFINED2 FROM BROKER_ACTIVE AS B)
)


SELECT
    POL_WITH_UD2.*,
    H_COMM.REGION AS FH_COMMISSIONINGAGT_REGION,
    H_COMM.MARKET AS FH_COMMISSIONINGAGT_MARKET,
    H_COMM.LOCATION AS FH_COMMISSIONINGAGT_LOCATION,
    H_COMM.USERDEFINED2 AS FH_COMMISSIONINGAGT_USERDEFINED2,
    H_SERV.REGION AS FH_SERVICINGAGT_REGION,
    H_SERV.MARKET AS FH_SERVICINGAGT_MARKET,
    H_SERV.LOCATION AS FH_SERVICINGAGT_LOCATION,
    H_SERV.USERDEFINED2 AS FH_SERVICINGAGT_USERDEFINED2,
    COALESCE(B_COMM.BROKERID, B_COMM_INACTIVE.BROKERID)
        AS FH_COMMISSIONINGAGT_BROKERID,
    --servicing agent work
    COALESCE(B_COMM.SEGMENTTAGWS, B_COMM_INACTIVE.SEGMENTTAGWS)
        AS FH_COMMISSIONINGAGT_SEGMENTTAGWS,
    COALESCE(B_COMM.COS_SALES_BDC, B_COMM_INACTIVE.COS_SALES_BDC)
        AS FH_COMMISSIONINGAGT_COS_SALES_BDC,
    COALESCE(B_COMM.COS_SALES_BDD, B_COMM_INACTIVE.COS_SALES_BDD)
        AS FH_COMMISSIONINGAGT_COS_SALES_BDD,
    COALESCE(B_COMM.COS_SALES_RVP, B_COMM_INACTIVE.COS_SALES_RVP)
        AS FH_COMMISSIONINGAGT_COS_SALES_RVP,
    COALESCE(B_COMM.AGENTNAME, B_COMM_INACTIVE.AGENTNAME)
        AS FH_COMMISSIONINGAGT_NAME,
    COALESCE(B_SERV.BROKERID, B_SERV_INACTIVE.BROKERID)
        AS FH_SERVICINGAGT_BROKERID,
    COALESCE(B_SERV.SEGMENTTAGWS, B_SERV_INACTIVE.SEGMENTTAGWS)
        AS FH_SERVICINGAGT_SEGMENTTAGWS,
    COALESCE(B_SERV.COS_SALES_BDC, B_SERV_INACTIVE.COS_SALES_BDC)
        AS FH_SERVICINGAGT_COS_SALES_BDC,
    COALESCE(B_SERV.COS_SALES_BDD, B_SERV_INACTIVE.COS_SALES_BDD)
        AS FH_SERVICINGAGT_COS_SALES_BDD,
    COALESCE(B_SERV.COS_SALES_RVP, B_SERV_INACTIVE.COS_SALES_RVP)
        AS FH_SERVICINGAGT_COS_SALES_RVP,
    COALESCE(B_SERV.AGENTNAME, B_SERV_INACTIVE.AGENTNAME)
        AS FH_SERVICINGAGT_NAME

FROM POL_WITH_UD2
LEFT JOIN HIER_W_UD2 AS H_COMM
    ON POL_WITH_UD2.FH_COMMISSIONINGAGTCODE = H_COMM.AGENTCODE
LEFT JOIN BROKER_ACTIVE AS B_COMM
    ON POL_WITH_UD2.FH_COMMISSIONINGAGT_UD2 = B_COMM.USERDEFINED2
LEFT JOIN REST_OF_BROKER_LIST AS B_COMM_INACTIVE
    ON POL_WITH_UD2.FH_COMMISSIONINGAGT_UD2 = B_COMM_INACTIVE.USERDEFINED2
LEFT JOIN HIER_W_UD2 AS H_SERV
    ON POL_WITH_UD2.FH_SERVICINGAGTCODE = H_SERV.AGENTCODE
LEFT JOIN BROKER_ACTIVE AS B_SERV
    ON POL_WITH_UD2.FH_SERVICINGAGT_UD2 = B_SERV.USERDEFINED2
LEFT JOIN REST_OF_BROKER_LIST AS B_SERV_INACTIVE
    ON POL_WITH_UD2.FH_SERVICINGAGT_UD2 = B_SERV_INACTIVE.USERDEFINED2
=======
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


>>>>>>> a8ace30bfa7db36b80ef8bf452f4b370dbc3f004
