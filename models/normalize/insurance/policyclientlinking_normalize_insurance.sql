 {{  config(alias='policyclientlinking', database='normalize', schema='insurance')  }} 


SELECT
  age,
  cdc_operation,
  changedby,
  changedbycode,
  changedon,
  clientcode,
  clientdob,
  clientfirstname,
  clientgender,
  clientlastname,
  clientmiddlename,
  clientsin,
  -- code,
  commit_timestamp,
  datalake_end_ts,
  datalake_start_ts,
  datalake_timestamp,
  is_first_known_record,
  is_last_known_record,
  policycode,
  relationship,
  share,
  smoker,
  -- stage_id,
  stream_position,
  sync_flag,
  transact_id,
  type,
  ws_pclcode
  
  
  ,datalake_timestamp as _infx_loaded_ts_utc  
  
            ,datalake_timestamp as _infx_active_from_ts_utc 
  
            ,case when datalake_end_ts = '3000-01-01 00:00:00.000' then '9999-12-31 23:59:59' else datalake_end_ts end as _infx_active_to_ts_utc  
  
            ,case when datalake_end_ts = '3000-01-01 00:00:00.000' then true else false end as _infx_is_active 
  

from {{ ref ('policyclientlinking_clean_insurance')  }}