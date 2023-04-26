 {{  config(alias='policystatus', database='normalize', schema='insurance')  }} 
 


SELECT
accrualflag,
  canadd,
  candel,
  canmod,
  category,
  cdc_operation,
  changedby,
  changedbycode,
  changedon,
  code,
  commissionrunupdatable,
  commit_timestamp,
  datalake_end_ts,
  datalake_start_ts,
  datalake_timestamp,
  englishdescription,
  frenchdescription,
  generaterequirements,
  indexsequence,
  is_first_known_record,
  is_last_known_record,
  note,
  stage_id,
  status,
  statusvalue,
  stream_position,
  transact_id,
  upddesc,
  updindex,
  updvalue
  
  ,_infx_loaded_ts_utc  
  ,_infx_active_from_ts_utc 
  ,_infx_active_to_ts_utc  
  ,_infx_is_active  
  
  


from {{ ref ('policystatus_clean_insurance')  }}