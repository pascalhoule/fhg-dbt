{{
    config(
        materialized="view",
        alias="__base_CL_Data",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]}
    )


WITH 

cl_extract as
(
select *, CONCAT(product_kind,'-',product_type) as cvg_name from {{ source('acdirect_sandbox', 'daily_insurance_ac_direct_agreement') }}),

fh_app_count as
(select 
current_contract_policy_number as policynumber,
1 as R,
count(distinct advisor_agreement_number) as c
from {{ source('acdirect_sandbox', 'daily_insurance_ac_direct_agreement') }}
group by 1),

CL_DATA_with_cvg as
(select 
current_contract_policy_number,
advisor_agreement_number,
CONCAT(product_kind,'-',product_type) AS cvg_name,
ROW_NUMBER() OVER (PARTITION BY current_contract_policy_number, advisor_agreement_number order by 
CONCAT(product_kind,'-',product_type)) AS coverage_number 
from {{ source('acdirect_sandbox', 'daily_insurance_ac_direct_agreement') }} cl
group by 1,2,3
),

CL_DATA_with_Rank_wgt as
(select 
current_contract_policy_number, advisor_agreement_number, 
1/fh_app_count.c as appcount
from CL_DATA_with_cvg  join fh_app_count on fh_app_count.policynumber = CL_DATA_with_cvg.current_contract_policy_number
group by all) ,



CL_Data_whole_table_w_cvg_num as
(select cl_extract.*, cvg.coverage_number, w.appcount AS FH_APPCOUNT
from cl_extract  
left join CL_DATA_with_cvg  cvg on cl_extract.current_contract_policy_number = cvg.current_contract_policy_number 
and cvg.cvg_name = cl_extract.cvg_name and cvg.advisor_agreement_number = cl_extract.advisor_agreement_number
left join CL_DATA_with_Rank_wgt w on w.current_contract_policy_number= cl_extract.current_contract_policy_number and w.advisor_agreement_number
= cl_extract.advisor_agreement_number 
where coverage_number = 1

UNION

select cl_extract.*, cvg.coverage_number, 0 as FH_APPCOUNT
from cl_extract  
left join CL_DATA_with_cvg  cvg on cl_extract.current_contract_policy_number = cvg.current_contract_policy_number 
and cvg.cvg_name = cl_extract.cvg_name and cvg.advisor_agreement_number = cl_extract.advisor_agreement_number
left join CL_DATA_with_Rank_wgt w on w.current_contract_policy_number= cl_extract.current_contract_policy_number and w.advisor_agreement_number
= cl_extract.advisor_agreement_number 
where coverage_number > 1
)

select 
ADVISOR_AGREEMENT_GROUP_IDENTIFIER,
FINANCIAL_HORIZONS_GROUP_IDENTIFIER,
ADVISOR_PRODUCER_NAME,
ADVISOR_AGREEMENT_NUMBER,
ADVISOR_REPORTING_FIRM_NAME,
AC_REGION,
AC_MARKET,
AC_LOCATION,
PRODUCT_KIND,
PRODUCT_TYPE,
CURRENT_CONTRACT_POLICY_NUMBER,
CARRIER,
CURRENT_POLICY_STATUS,
SETTLEMENT_DATE,
APPLICATION_DATE,
FIRST_COMMISSION_DATE,
PLACED_TOTAL_SALES_MEASURE,
PENDING_DECIDED_TOTAL_SALES_MEASURE,
CASE WHEN CURRENT_POLICY_STATUS = '-' THEN 0 ELSE SALES_COUNT_CREDIT END AS SALES_COUNT_CREDIT,
CASE WHEN CURRENT_POLICY_STATUS = '-' THEN 0 ELSE PLACED_POLICY_COUNT END AS PLACED_POLICY_COUNT,
CASE WHEN CURRENT_POLICY_STATUS = '-' THEN 0 ELSE PENDING_POLICY_COUNT END AS PENDING_POLICY_COUNT,
FYC_AMOUNT,
CVG_NAME,
COVERAGE_NUMBER,
FH_APPCOUNT
FROM CL_Data_whole_table_w_cvg_num
