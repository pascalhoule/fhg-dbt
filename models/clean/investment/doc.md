{% docs aua_vc %}

Spoke with Heather Leblanc on 2024-08-16
Decision was made to accept the cost of fully refreshing this data daily.  No incremental model necessary to reduce costs.

As of 2024-0816 this is the largest table in our database.

This code works to implement an incremental model to bring in the data as at the first of the month.  The daily data for other dates is excluded.
I took it out of production after it has been tested.

--{{
--  config( 
--    alias='aua_vc_me', 
--    database='clean', 
--    schema='investment',
--    materialized = 'incremental',
--    unique_key = ['repcode', 'fundproductcode', 'trenddate'],
--    merge_update_columns = ['marketvalue']
--  )
--}}


--select *
--from {{ source('investment_curated', 'aua_vc') }}
--where
--    date_part(day, trenddate) = 1

--{% if is_incremental() %}
--    and trenddate >= (select dateadd(day,-366, max(trenddate)) from {{ this }})
--{% endif %}

{% enddocs %}


{% docs transactions_vc %}

Spoke with Heather Leblanc on 2024-08-16
Decision was made to accept the cost of fully refreshing this data daily.  No incremental model necessary to reduce costs.

As of 2024-0816 this is the second largest table in our database.

This code works to bring in all the data and incrementally update the most recent 14 days.

--{{
--  config( 
--    alias='transactions_vc', 
--    database='clean', 
--    schema='investment',
--    materialized = 'incremental',
--    unique_key = 'transactioncode',
--  )
--}}


--select *
--from {{ source('investment_curated', 'transactions_vc') }}
--where
    

--{% if is_incremental() %}
--    tradedate >= (select dateadd(day,-14, max(tradedate)) from {{ this }})
--{% endif %}

{% enddocs %}