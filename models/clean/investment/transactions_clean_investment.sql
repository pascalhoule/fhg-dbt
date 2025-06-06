 {{  config(alias='transactions', database='clean', schema='investment')  }} 


SELECT * FROM {{ source ('investment_raw', 'transactions')  }}

-- with 
-- clean_trans as 
-- (select
-- code 
-- ,fundaccount_code
-- ,amount
-- ,average_cost
-- ,beartempcode
-- ,trade_date
-- ,setlement_date
-- ,dealer_commission
-- ,currencyname
-- ,fundproduct_name
-- ,net_amount
-- ,paymentid
-- ,settlementamount
-- ,shares_units
-- ,share_balance_after 
-- ,unit_price
-- ,ext_type_code 
-- ,payment_status 
-- ,settlement_period
-- ,cancel_flag 
-- ,transaction_flag
-- ,datalake_timestamp 
-- ,'insert' as cdc_operation
-- from {{ this }}
-- ),
-- raw_delta as 
-- (
--     select 
--  code 
-- ,fundaccount_code
-- ,amount
-- ,average_cost
-- ,beartempcode
-- ,trade_date
-- ,setlement_date
-- ,dealer_commission
-- ,currencyname
-- ,fundproduct_name
-- ,net_amount
-- ,paymentid
-- ,settlementamount
-- ,shares_units
-- ,share_balance_after 
-- ,unit_price
-- ,ext_type_code 
-- ,payment_status 
-- ,settlement_period
-- ,cancel_flag 
-- ,transaction_flag
-- ,datalake_timestamp 
-- ,cdc_operation 
--     from {{ source ('investment_raw', 'transactions')  }}
--     where datalake_timestamp > (select max(clean_trans.datalake_timestamp) from clean_trans)
    
-- ),
-- uni as (
-- select *
-- from clean_trans
-- union all
-- select *
-- from raw_delta
-- ), add_rn as (

-- select *,
-- row_number() over( partition by code order by datalake_timestamp desc  ) as rn
-- from uni
-- )
-- select * exclude(rn) from add_rn
-- where rn = 1
-- and cdc_operation <> 'delete'


