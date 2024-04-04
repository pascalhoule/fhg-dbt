 {{  config(alias='fundaccount', database='report', schema='investment')  }} 


select 
INVESTMENT_FUNDACCOUNT_CODE as "Fundaccount Code",
INVESTMENT_FUNDACCOUNT_ACCOUNT_NUMBER as "Fundaccount Account Number",
INVESTMENT_FUNDACCOUNT_CURRENT_VALUE as "Fundaccount Current Value",
INVESTMENT_FUNDACCOUNT_TOTAL_SHARES_ISSUED as "Fundaccount Total Shares Issued",
INVESTMENT_FUNDACCOUNT_TRADE_DATE_MARKET_VALUE as "Fundaccount Trade Date Market Value",
INVESTMENT_FUNDACCOUNT_UNISSUEDUNITS as "Fundaccount Unissuedunits",
INVESTMENT_FUND_PRODUCTS_CLASS as "Fund Products Class",
INVESTMENT_FUND_PRODUCTS_CURRENCY as "Fund Products Currency",
INVESTMENT_FUND_PRODUCTS_CUSIP as "Fund ID",
INVESTMENT_FUND_PRODUCTS_LOAD_TYPE as "Fund Products Load Type",
investment_fund_products_load_type_fr as "Fund Products Load Type Fr",
INVESTMENT_FUND_PRODUCTS_NAME as "Fund Products Name",
INVESTMENT_FUND_PRODUCTS_PRICE as "Fund Products Price",
INVESTMENT_FUND_PRODUCTTYPES_PRODUCTTYPENAME as "Fund Producttypes Producttypename",
INVESTMENT_FUND_PRODUCTTYPES_SUBTYPENAME as "Fund Producttypes Subtypename",
INVESTMENT_FUND_SPONSOR_NAME as "Fund Sponsor Name",
INVESTMENT_FUND_SPONSOR_ALIAS as "Fund Sponsor Alias",
INVESTMENT_REGISTRATION_CODE as "Registration Code",
INVESTMENT_REGISTRATION_KYC_CODE as "Registration Kyc Code",
INVESTMENT_REGISTRATION_REGISTRATION_TYPE as "Registration Registration Type",
accounttype,
investment_fund_beneficiaries as "Investment Fund Beneficiaries"
from {{ ref ('fundaccount_analyze_investment_consultant')  }} 