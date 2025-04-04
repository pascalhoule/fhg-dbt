 {{  config(alias='transactions', database='report_cl', schema='investment')  }} 

SELECT * FROM {{ ref('transactions_analyze_investment') }}
--select 
--INVESTMENT_TRANSACTIONS_AMOUNT as "Amount",
--INVESTMENT_TRANSACTIONS_AVERAGE_COST as "Average Cost",
--INVESTMENT_TRANSACTIONS_BEARTEMPCODE as "Beartemp Code",
--INVESTMENT_TRANSACTIONS_CODE as "Code",
--INVESTMENT_TRANSACTIONS_CURRENCYNAME as "Currency Name",
--INVESTMENT_TRANSACTIONS_DEALER_COMMISSION as "Dealer Commission",
--INVESTMENT_TRANSACTIONS_FUNDACCOUNT_CODE as "Fundaccount Code",
--INVESTMENT_TRANSACTIONS_FUNDPRODUCT_NAME as "Fundproduct Name",
--INVESTMENT_TRANSACTIONS_NET_AMOUNT as "Net Amount",
--INVESTMENT_TRANSACTIONS_PAYMENTID as "Payment Id",
--INVESTMENT_TRANSACTIONS_PAYMENT_STATUS as "Payment Status",
--investment_transactions_payment_status_fr as "Payment Status Fr",
--INVESTMENT_TRANSACTIONS_SETLEMENT_DATE as "Setlement Date",
--INVESTMENT_TRANSACTIONS_SETTLEMENTAMOUNT as "Settlement Amount",
--INVESTMENT_TRANSACTIONS_SETTLEMENT_PERIOD as "Settlement Period",
--INVESTMENT_TRANSACTIONS_SETTLEMENT_PERIOD_FR as "Settlement Period Fr",
--INVESTMENT_TRANSACTIONS_SHARES_UNITS as "Shares Units",
--INVESTMENT_TRANSACTIONS_SHARE_BALANCE_AFTER as "Share Balance After",
--INVESTMENT_TRANSACTIONS_TRADE_DATE as "Trade Date",
--INVESTMENT_TRANSACTIONS_UNIT_PRICE as "Unit Price",
--INVESTMENT_TRANSACTIONTYPES_DISPLAY_NAME as "Transactiontypes Display Name"
--from {{ ref ('transactions_analyze_investment')  }} 
--where INVESTMENT_TRANSACTIONS_TRADE_DATE > dateadd('MONTH', -6, current_date())