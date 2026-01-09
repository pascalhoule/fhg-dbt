{{  config(alias='loi_tracking', database='normalize', schema='backups', materialized = "table", transient = false)  }} 

select * from {{ source('app_tables', 'loi_tracking') }}