{{  config(alias='loi_tracking', database='normalize', schema='backups', materialized = "table")  }} 

select * from {{ source('app_tables', 'loi_tracking') }}