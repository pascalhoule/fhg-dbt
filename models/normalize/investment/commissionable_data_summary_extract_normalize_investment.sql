{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract', 			
        database='normalize', 			
        schema='investment'  		
    )			
}}


with src as (
    -- Base August extract
    select
        TRANSACTION_CODE,
        TRANSACTION_REPID,
        TRANSACTION_REP_NAME,
        "Account #"        as account_no,
        SPONSOR_ID         as sponsor_id,
        SPONSOR            as sponsor_name,
        CUSIP,
        PRODUCT_NAME,
        LOAD_TYPE          as blue_sun_load_type_in,
        INVESTMENT_AMOUNT  as investment_amount,
        COMMISSION_RUN_ID,
        COMMISSION_RUN_START_DATE,
        COMMISSION_RUN_END_DATE,
        COMMISSION_CHEQUE_DATE,
        "Sub-Region"       as sub_region_raw,
        COMMISSION_REP_NAME,
        GROUP_LEADER_NAME,
        NET_COMMISSION
    from  {{ ref('commissionable_data_summary_extract_transaction_normalize_investment') }} 
),

-- ------------------------------------------------------------
-- 1) Branch derivations (Branch1/2/3) + Category
-- ------------------------------------------------------------
b1 as (
    select
        s.*,

        /* Branch 1 */
        case
            when s.commission_rep_name ilike '% LOC%' 
              or s.commission_rep_name ilike '%DEPT%'
              or lower(s.commission_rep_name) = 'vancouver island (victoria)'
                then s.commission_rep_name
            when upper(s.commission_rep_name) like '%MAP - W%'
                then 'WSE'
            when upper(s.commission_rep_name) like '%MAP - F%'
                then 'F55'
            else null
        end as branch_1,

        /* Dhaval Power Query replaces null with 'test' */
        coalesce(s.group_leader_name, 'test') as gl_name_fix
    from src s
),

b2 as (
    select
        b1.*,

        /* Branch 2 */
        case
            when branch_1 in ('WSE','F55') then null
            when sub_region_raw ilike '%Brokers%' then sub_region_raw
            else null
        end as branch_2
    from b1
),

b3 as (
    select
        b2.*,

        /* Branch 3 */
        coalesce(branch_1, branch_2) as branch_3
    from b2
),

cat as (
    select
        b3.*,

        /* Category base */
        case
            when branch_3 in ('Brampton Branch','Peel Branch') then null
            when branch_3 = 'WSE' then 'WSE'
            when branch_3 = 'F55' then 'F55'
            when branch_3 ilike '%Brokers%' then 'Broker'
            else 'Branch'
        end as categ_base,

        /* Peel Branch flag */
        case
            when gl_name_fix ilike '%Brampton Share%' then 'Peel Branch'
            when gl_name_fix ilike '%Peel  Share%'    then 'Peel Branch'
            else null
        end as peel_branch
    from b3
),

cat2 as (
    select
        cat.*,

        /* Final Category = concat(peel_branch, categ_base) */
        concat(coalesce(peel_branch,''), coalesce(categ_base,'')) as category
    from cat
),

-- ------------------------------------------------------------
-- 2) Revenue buckets per row (Broker/Branch/Region/MAP)
-- ------------------------------------------------------------
rev_rows as (
    select
        cat2.*,

        case when category = 'Broker' then net_commission else 0 end as broker_fyc,
        case when category in ('Branch','Brampton BranchBroker','Peel BranchBroker')
             then net_commission else 0 end as branch_revenue,
        case when category = 'MGA'
               or gl_name_fix in ('Brampton Branch','Peel Branch')
             then net_commission else 0 end as region_revenue,

        case when category = 'WSE' then net_commission else 0 end as map_wse,
        case when category = 'F55' then net_commission else 0 end as map_f55,

        /* Branch carried only for Branch / Peel BranchBroker */
        case when category in ('Branch','Peel BranchBroker') then branch_3 else null end as branch_for_fill
    from cat2
),

-- ------------------------------------------------------------
-- 3) Fill down Branch 3 inside grouping
-- ------------------------------------------------------------
filled as (
    select
        r.*,
        max(branch_for_fill) over (
            partition by TRANSACTION_CODE
        ) as branch_3_filled
    from rev_rows r
),

-- ------------------------------------------------------------
-- 4) Group to advisor/account/cusip/run granularity
--     NOTE: must keep SponsorID for later carrier lookup
-- ------------------------------------------------------------
grp as (
    select
        max(TRANSACTION_REPID)                     as broker_id,
        max(TRANSACTION_REP_NAME)             as broker_name,
        max(account_no)                       as account_no,
        max(CUSIP)                            as CUSIP,
        max(PRODUCT_NAME)                     as product_name,
        max(blue_sun_load_type_in)            as blue_sun_load_type,
        max(COMMISSION_RUN_ID)                as COMMISSION_RUN_ID,
        max(COMMISSION_CHEQUE_DATE)           as commission_cheque_date,
        max(branch_3_filled)                  as branch_3,
        max(sponsor_id)                       as sponsor_id,   

        max(investment_amount)              as deposit,
        sum(broker_fyc)                     as broker_fyc_1,
        sum(branch_revenue)                 as branch_revenue_1,
        sum(region_revenue)                 as region_revenue_1,
        sum(map_wse)                        as map_wse_rev,
        sum(map_f55)                        as map_f55_rev
    from filled
    group by
        TRANSACTION_CODE
),

-- ------------------------------------------------------------
-- 5) MAP + Branch Revenue and Retail flag
-- ------------------------------------------------------------
mapfix as (
    select
        g.*,
        (branch_revenue_1 + map_wse_rev + map_f55_rev) as branch_revenue_final,
        case
            when map_wse_rev <> 0 then 'WSE'
            when map_f55_rev <> 0 then 'F55'
            else null
        end as retail
    from grp g
),

-- ------------------------------------------------------------
-- 6) Dedup lookups to avoid row multiplication
-- ------------------------------------------------------------
lk_loc_dedup as (
    select
        COMMISSION_REP_NAME,
        max(FHG_BRANCH) as FHG_BRANCH
    from {{ source('norm', 'location_fin_fh') }} 
    group by COMMISSION_REP_NAME
),

lk_reg_dedup as (
    select
        BRANCH,
        max(REGION) as REGION
    from {{ source('norm', 'region_fin_fh') }} 
    group by BRANCH
),

lk_car_dedup as (
    select
        SPONSOR_ID,
        max(CARRIER_DATABASE_NAME) as CARRIER_DATABASE_NAME
    from {{ source('norm', 'carrier_fin_fh') }}   
    group by SPONSOR_ID
),

lk_lt_dedup as (
    select
        OLDNAME,
        max(NEWTYPE) as NEWTYPE
    from {{ source('norm', 'loadtypes_fin_fh') }}   
    group by OLDNAME
),

-- ------------------------------------------------------------
-- 7) Location -> FHG Branch, Region, Carrier, LoadType mapping
-- ------------------------------------------------------------
loc as (
    select
        m.*,
        lk_loc.FHG_BRANCH as fhg_branch
    from mapfix m
    left join lk_loc_dedup lk_loc
        on m.branch_3 = lk_loc.COMMISSION_REP_NAME
),

reg as (
    select
        l.*,
        lk_reg.REGION
    from loc l
    left join lk_reg_dedup lk_reg
        on l.fhg_branch = lk_reg.BRANCH
),

car as (
    select
        r.*,
        coalesce(lk_car.CARRIER_DATABASE_NAME, 'none') as carrier_raw
    from reg r
    left join lk_car_dedup lk_car
        on r.sponsor_id = lk_car.SPONSOR_ID
),

lt as (
    select
        c.*,
        coalesce(lk_lt.NEWTYPE, c.blue_sun_load_type) as load_type_mapped
    from car c
    left join lk_lt_dedup lk_lt
        on trim(c.product_name) = trim(lk_lt.OLDNAME)
),

-- ------------------------------------------------------------
-- 8) Total FYC, Carrier cleanup, filters, Deposit fix
-- ------------------------------------------------------------

base_final as (
    select
        broker_id,
        replace(broker_name, '.', '') as broker_name,
        fhg_branch as branch,
        cast(null as varchar) as sub_branch,
        region,
        account_no as "Account #",
        CUSIP      as "Fund ID",
        product_name as "Product Name",

        case
            when carrier_raw = 'SSQ Life' then 'SSQ'
            when carrier_raw = 'SunLife'  then 'Sun Life'
            when carrier_raw = 'SSO Life' then 'SSO'
            else carrier_raw
        end as carrier,

        broker_fyc_1         as "Broker FYC",
        branch_revenue_final as "Branch Revenue",
        region_revenue_1     as "Region Revenue",
        (broker_fyc_1 + branch_revenue_final + region_revenue_1) as total_fyc,

        deposit,
        load_type_mapped as "Blue Sun Load Type",
        commission_cheque_date,
        retail
    from lt
    where (broker_fyc_1 + branch_revenue_final + region_revenue_1 + deposit) <> 0
),

-- NEW: net/group “finance-style” before deposit_fix / myload
base_final_fin as (
    select
        broker_id,
        broker_name,
        branch,
        sub_branch,
        region,
        carrier,

        month(commission_cheque_date) as period,
        year(commission_cheque_date)  as year,

        "Account #",
        "Fund ID",
        "Product Name",
        "Blue Sun Load Type",
        retail,

        sum("Broker FYC")      as "Broker FYC",
        sum("Branch Revenue")  as "Branch Revenue",
        sum("Region Revenue")  as "Region Revenue",
        sum("Broker FYC") + sum("Branch Revenue") + sum("Region Revenue") as total_fyc,

        sum(deposit) as deposit,

        max(commission_cheque_date) as commission_cheque_date
    from base_final
    group by
        broker_id, broker_name, branch, sub_branch, region, carrier,
        month(commission_cheque_date), year(commission_cheque_date),
        "Account #", "Fund ID", "Product Name", "Blue Sun Load Type", retail
),

deposit_fix as (
    select
        b.*,
        case when total_fyc < 0 and deposit > 0 then -deposit else deposit end as deposit_fix
    from base_final_fin b
),

-- ------------------------------------------------------------
-- 9) My Load Type rules (FYC % tests)
-- ------------------------------------------------------------

load_calc as (
    select
        d.*,
        total_fyc / nullif(deposit_fix, 0) as fyc_pct
    from deposit_fix d
),


myload as (
    select
        l.*,
        case
            when fyc_pct = 0 then 'No Load'
            when fyc_pct > 0 and fyc_pct < 0.04 then 'Low Load'
            when fyc_pct >= 0.04 and fyc_pct <= 0.99 then 'DSC'
            when total_fyc > 0 and deposit_fix = 0 then 'Low Load'
            else "Blue Sun Load Type"
        end as my_load_type_raw
    from load_calc l
),

myload_fix as (
    select
        m.*,
        case
            when my_load_type_raw ilike 'Back Load%'  then 'DSC'
            when my_load_type_raw ilike 'Front Loa%' then 'DSC'
            when my_load_type_raw is null or my_load_type_raw = '' then 'DSC'
            else my_load_type_raw
        end as "My Load Type"
    from myload m
),

-- ------------------------------------------------------------
-- 10) Final rounding + Year/Period + constants
-- ------------------------------------------------------------
clean_final as (
    select
        broker_id,
        broker_name,
        branch,
        sub_branch,
        region,
        'FY Commission' as "Trans Type",
        carrier as "Carrier",
        round("Broker FYC", 2)      as "Broker FYC",
        round("Branch Revenue", 2)  as "Branch Revenue",
        round("Region Revenue", 2)  as "Region Revenue",
        round(total_fyc, 2)         as "Total FYC",
        period as "Period",
        year   as "Year",
        "Account #",
        "Fund ID",
        "Product Name",
        round(deposit_fix, 2)       as "Deposit",
        "Blue Sun Load Type",
        "My Load Type",
        'WS Investments' as "Source",
        'Actual'         as "Ledger",
        retail           as "Retail"
    from myload_fix
    where round("Broker FYC",2) + round("Branch Revenue",2) + round("Region Revenue",2)
          + round(total_fyc,2) + round(deposit_fix,2) <> 0
)
,

-- ------------------------------------------------------------
-- 11) Apply BrokerSplit2 using Joint_id_rate (splits done last)
-- ------------------------------------------------------------
split_join as (
    select
        cf.*,
--try_to_number(jr.REPID)::number(38,0)   as new_broker_id,
 jr.repid::text as new_broker_id,
trim(concat_ws(' ', jr.LAST_NAME, jr.FIRST_NAME)) as new_broker_name,
        coalesce(jr.SHARE / 100.0, 1.0) as split_pct
    from clean_final cf
    left join {{ ref('joint_id_rate_fh_normalize_investment') }} jr
        on try_to_number(cf.broker_id) = try_to_number(jr.JOINTID)
),

split_applied as (
    select
--to_varchar(coalesce(new_broker_id, broker_id)::number(38,0)) as "Broker ID",
  to_varchar(coalesce(new_broker_id, broker_id)::text) as "Broker ID",
        coalesce(new_broker_name, broker_name)         as "Broker Name",

        branch as "Branch",
        sub_branch as "Sub-Branch",
        region as "Region",
        "Trans Type",
        "Carrier",

        round("Broker FYC"     * split_pct, 2) as "Broker FYC",
        round("Branch Revenue" * split_pct, 2) as "Branch Revenue",
        round("Region Revenue" * split_pct, 2) as "Region Revenue",
        round("Total FYC"      * split_pct, 2) as "Total FYC",
        "Period",
        "Year",

        "Account #",
        "Fund ID",
        "Product Name",
        round("Deposit" * split_pct, 2) as "Deposit",

        "Blue Sun Load Type",
        "My Load Type",
        "Source",
        "Ledger",
        "Retail"
    from split_join
)

select * 
from split_applied
