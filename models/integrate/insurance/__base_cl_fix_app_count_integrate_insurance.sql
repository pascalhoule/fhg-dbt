{{
    config(
        materialized="view",
        alias="__base_cl_fix_app_count",
        database="integrate",
        schema="insurance",
        grants={"ownership": ["FH_READER"]}
    )
}}


SELECT
true as USE_FH_APPCOUNT,
current_contract_policy_number, 
sum(sales_count_credit) AS incorrect_count
from
{{ ref('__base_CL_Data_integrate_insurance') }}
where current_policy_status in ('Placed', '-') 
group by 1, 2
having sum(sales_count_credit) != 1

UNION

SELECT
true as USE_FH_APPCOUNT,
current_contract_policy_number, 
sum(pending_policy_count) AS incorrect_count
from
{{ ref('__base_CL_Data_integrate_insurance') }}
where current_policy_status in ('Pending','Decided') 
group by 1, 2
having sum(pending_policy_count) != 1
