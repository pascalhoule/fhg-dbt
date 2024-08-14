{{
  config( 
    alias='aua_vc_me_clean_investment', 
    database='clean', 
    schema='investment',
    materialized = 'incremental',
    unique_key = ['repcode', 'fundproductcode', 'trenddate'],
    merge_update_columns = ['marketvalue']
  )
}}


select *
from {{ source('investment_curated', 'aua_vc') }}
where
    date_part(day, trenddate) = 1

{% if is_incremental() %}
    and trenddate >= (selectdateadd(day,-366, max(trenddate) from {{ this }})
{% endif %}

