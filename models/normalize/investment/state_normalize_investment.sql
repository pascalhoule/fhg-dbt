 {{  config(alias='state', database='normalize', schema='investment')  }} 


SELECT
 cdc_operation as investment_state_cdc_operation
,changedby as investment_state_changedby
,changedon as investment_state_changedon
,code as investment_state_code
,commit_timestamp as investment_state_commit_timestamp
,datalake_end_ts as investment_state_datalake_end_ts
,datalake_start_ts as investment_state_datalake_start_ts
,datalake_timestamp as investment_state_datalake_timestamp
,is_first_known_record as investment_state_is_first_known_record
,is_last_known_record as investment_state_is_last_known_record
,name as investment_state_name
,stage_id as investment_state_stage_id
,state_type as investment_state_state_type
,stream_position as investment_state_stream_position
,transact_id as investment_state_transact_id
,_infx_loaded_ts_utc 
,_infx_active_from_ts_utc 
,_infx_active_to_ts_utc  
,_infx_is_active  
  


from {{ ref ('state_clean_investment')  }}