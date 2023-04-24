 {{ config(alias='state', database='normalize', schema='insurance') }}

select
    code as state_id
    ,name as state_name
    ,_infx_loaded_ts_utc 
    ,_infx_active_from_ts_utc
    ,_infx_active_to_ts_utc 
    ,_infx_is_active

from {{ ref ('state_clean_insurance') }}
