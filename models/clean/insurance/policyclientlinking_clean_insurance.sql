 {{  config(alias='policyclientlinking', database='clean', schema='insurance')  }} 


SELECT 
distinct
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
  commit_timestamp::string::timestamp as commit_timestamp,
  datalake_end_ts::string::timestamp as datalake_end_ts,
  datalake_start_ts::string::timestamp as datalake_start_ts,
  datalake_timestamp::string::timestamp as datalake_timestamp,
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



from {{ source ('insurance', 'policyclientlinking')  }}