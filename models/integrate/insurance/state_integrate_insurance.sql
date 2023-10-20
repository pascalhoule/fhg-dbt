 {{ config(alias='state', database='integrate', schema='insurance') }}

select
    state_id
    ,state_name
    ,_infx_loaded_ts_utc 
    ,_infx_active_from_ts_utc
    ,_infx_active_to_ts_utc 
    ,_infx_is_active
    ,'tt' as test_field

from {{ ref ('state_normalize_insurance') }} c
