 {{  config(alias='constants', database='normalize', schema='insurance')  }} 
 
 
SELECT 
  cdc_operation,
  changedby,
  changedbycode,
  changedon,
  commit_timestamp,
  condition,
  constantid,
  constantkey,
  constanttypeid,
  datalake_end_ts,
  datalake_start_ts,
  datalake_timestamp,
  description,
  descriptionfr,
  indexsequence,
  is_first_known_record,
  is_last_known_record,
  note,
  stage_id,
  stream_position,
  transact_id,
  type,
  value
  ,_infx_loaded_ts_utc  
  ,_infx_active_from_ts_utc 
  ,_infx_active_to_ts_utc  
  ,_infx_is_active  
  
 
from {{ ref ('constants_clean_insurance')  }}