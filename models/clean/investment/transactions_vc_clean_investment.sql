{{
  config( 
    alias='transactions_vc', 
    database='clean', 
    schema='investment',
    materialized = 'incremental',
    unique_key = 'transactioncode',
  )
}}


select *
from {{ source('investment_curated', 'transactions_vc') }}
where
    

{% if is_incremental() %}
    tradedate >= (select dateadd(day,-366, max(tradedate)) from {{ this }})
{% endif %}