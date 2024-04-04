 {{  config(alias='registration', database='clean', schema='investment')   }} 


SELECT * 
  ,datalake_timestamp as _infx_loaded_ts_utc  
  
            ,datalake_timestamp as _infx_active_from_ts_utc 
  
            ,case when datalake_end_ts = '3000-01-01 00:00:00.000' then '9999-12-31 23:59:59' else datalake_end_ts end as _infx_active_to_ts_utc  
  
            ,case when datalake_end_ts = '3000-01-01 00:00:00.000' then true else false end as _infx_is_active 
  


from {{ source ('investment', 'registration') }}