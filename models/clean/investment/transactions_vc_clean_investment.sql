 {{  config(alias='transactions_vc', database='clean', schema='investment')  }} 


SELECT *
FROM {{ source('investment_curated', 'transactions_vc') }}
