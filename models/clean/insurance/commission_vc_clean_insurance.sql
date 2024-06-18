{{  config(alias='commission_vc', database='clean', schema='insurance')  }} 


SELECT * 
  ,null as _infx_loaded_ts_utc  
  
            ,null as _infx_active_from_ts_utc 
  
            ,'9999-12-31 23:59:59' as _infx_active_to_ts_utc  
  
            ,null as _infx_is_active 
  
from {{ source ('insurance_curated', 'commission_vc')  }}