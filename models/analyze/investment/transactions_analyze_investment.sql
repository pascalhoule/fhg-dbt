 {{  config(alias='transactions', database='analyze', schema='investment')  }} 


SELECT 
 tr.investment_transactions_code
,tr.investment_transactions_fundaccount_code
,tr.investment_transactions_amount
,tr.investment_transactions_average_cost
,tr.investment_transactions_beartempcode
,tr.investment_transactions_trade_date
,tr.investment_transactions_setlement_date
,tr.investment_transactions_dealer_commission
,tr.investment_transactions_currencyname

,tr.investment_transactions_fundproduct_name
,tr.investment_transactions_net_amount
,tr.investment_transactions_paymentid
,c_ps.description as  investment_transactions_payment_status
,c_ps.description2 as investment_transactions_payment_status_fr
,tr.investment_transactions_settlementamount
,c_sp.description as investment_transactions_settlement_period
,c_sp.description2 as investment_transactions_settlement_period_fr
,tr.investment_transactions_shares_units
,tr.investment_transactions_share_balance_after
,tr.investment_transactions_unit_price
-- ,tr.investment_transactions_rep_code
,tt.investment_transactiontypes_display_name

from {{ ref ('transactions_integrate_investment')  }} tr
inner join {{ ref ('fundaccount_normalize_investment_consultant')  }} fa on tr.investment_transactions_fundaccount_code = fa.investment_fundaccount_code
left join {{ ref ('transactiontypes_integrate_investment')  }} tt on tr.investment_transactions_ext_type_code = tt.investment_transactiontypes_ext_type_code
left join {{ ref ('constants_integrate_investment') }} c_ps on c_ps.type = 'PaymentStatus' and c_ps.value = tr.investment_transactions_payment_status
left join {{ ref ('constants_integrate_investment') }} c_sp on c_sp.type = 'Settlement_Period' and c_sp.value = tr.investment_transactions_settlement_period
    
where 
    tr.investment_transactions_cancel_flag = 0
AND tr.investment_transactions_transaction_flag > 2 --settled
AND tr.investment_transactions_ext_type_code NOT IN (350, 351) --excludes adjustments
