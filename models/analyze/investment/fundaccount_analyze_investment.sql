 {{  config(alias='fundaccount', database='analyze', schema='investment')  }} 

with beneficiaries as (
select 
registrationcode, 
ARRAY_TO_STRING( array_agg(first_name ||' ' || last_name || ' ' || ifnull(cast(percentage as string), 'n/a') || '%'), ', ') as beneficiaries
from {{ ref ('beneficiaries_integrate_investment') }}
group by registrationcode
)
select
 reg.investment_registration_code
,reg.investment_registration_registration_type
,reg.investment_registration_kyc_code
,fa.investment_fundaccount_code
,fa.investment_fundaccount_current_value
,fa.investment_fundaccount_total_shares_issued
,fa.investment_fundaccount_unissuedunits
,fa.investment_fundaccount_trade_date_market_value
,fa.investment_fundaccount_account_number

,fp.investment_fund_products_class
,fp.investment_fund_products_currency
,fp.investment_fund_products_cusip
-- ,fp.investment_fund_products_load_type
,loadtype.description as investment_fund_products_load_type
,loadtype.description2 as investment_fund_products_load_type_fr
,fp.investment_fund_products_name
,fp.investment_fund_products_price
,pt.investment_fund_producttypes_producttypename
,pt.investment_fund_producttypes_subtypename
,s.investment_fund_sponsor_name
,s.investment_fund_sponsor_alias
,accounttype.description AS accounttype
,b.beneficiaries as investment_fund_beneficiaries

from {{ ref ('registration_integrate_investment')  }} reg 
left join {{ ref ('constants_integrate_investment')  }} accounttype 
        ON reg.INVESTMENT_REGISTRATION_REGISTRATION_TYPE ilike accounttype.value 
        AND accounttype.type ilike 'accounttype'
inner join {{ ref ('fundaccount_integrate_investment')  }} fa on fa.investment_fundaccount_registration_code = reg.investment_registration_code

-- inner join {{ ref ('registration_integrate_investment')  }} reg on reg.code = fa.registration_code
-- inner join {{ ref ('clients_integrate_investment')  }} cl on cl.code = reg.kyc_code
left join {{ ref ('fund_products_integrate_investment')  }} fp on fp.investment_fund_products_code = fa.investment_fundaccount_fundproduct_code
left join {{ ref ('constants_integrate_investment') }} loadtype on loadtype.value = fp.investment_fund_products_load_type and loadtype.type = 'LoadType'
left join {{ ref ('producttypes_integrate_investment')  }} pt on pt.investment_fund_producttypes_fund_subtype = fp.investment_fund_products_fund_subtype
left join {{ ref ('sponsor_integrate_investment')  }} s on s.investment_fund_sponsor_code = fp.investment_fund_products_sponsor_code
left join beneficiaries b on b.registrationcode = reg.investment_registration_code
