{{			
    config (			
        materialized="view",			
        alias='policy_fh_cl', 			
        database='integrate', 			
        schema='insurance',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

Select * from
{{ ref('policy_fh_integrate_insurance') }}

UNION ALL

Select 
null as FH_POLICYCATEGORY,
null as POLICYCODE,
null as	POLICYGROUPCODE,
null as	FH_SERVICINGAGTCODE,
null as FH_SERVICINGAGTSPLIT,
cast(ADVISOR_AGREEMENT_GROUP_IDENTIFIER as VARCHAR) as	FH_COMMISSIONINGAGTCODE,
null as	FH_COMMISSIONINGAGTSPLIT,
'CL Direct' as	FH_CARRIERENG,
'CL Direct'	as FH_CARRIERFR,
CURRENT_CONTRACT_POLICY_NUMBER as POLICYNUMBER,
null as	PLANID,
product_kind as FH_PLANTYPE,
product_type as FH_PLANNAMEENG,
product_type as	FH_PLANNAMEFR,
null as	PREMIUMAMOUNT,
null as	ANNUALPREMIUMAMOUNT,
null as	COMMPREMIUMAMOUNT,
null as	PAYMENTMODE,
null as	FACEAMOUNT,
null as	CREATEDBY,
null as	CREATEDDATE,
null as	CONTRACTDATE,
null as	SETTLEMENTDATE,
null as	EXPIRYDATE,
null as	RENEWALDATE,
null as	SENTTOICDATE,
null as	MAILEDDATE,
null as	FH_FINPOSTDATE,
null as	FH_STATUSCODE,
null as	FH_STATUSNAMEENG,
null as	FH_STATUSNAMEFR,
current_policy_status as FH_STATUSCATEGORY,
policy_count as APPCOUNT,
null as	FH_FYCSERVAMT,
try_cast (Replace ( replace (fyc_amount, '$',''), ',','') as float) as FH_FYCCOMMAMT,
null as	MGAFYOAMOUNT,
null as	ISSUEPROVINCE,
null as	FH_APPSOURCE,
null as	FH_APPTYPE,
null as	LASTCOMMISSIONPROCESSDATE,
null as	FIRSTOWNERCLIENTCODE,
null as	FIRSTINSUREDCLIENTCODE,
null as	ISMAINCOVERAGE,
try_cast (SETTLEMENT_DATE as Date) as FH_SETTLEMENTDATE,
null as	FH_STARTDATE,
null as	FH_PREMIUM,
try_cast (Replace(replace(placed_total_sales_measure, '$',''),',','') as float) + try_cast (Replace(replace(pending_decided_total_sales_measure, '$',''),',','') as float) as FH_PREM_COMMWGT,
null as	FH_PREM_SERVWGT,
try_cast (application_date as date) as APPLICATIONDATE,
try_cast (first_commission_date as date) as FH_PLACEDDATE,
null as	FH_FYCPLACED

from
{{ source('acdirect', 'daily_insurance_acdirect') }}
