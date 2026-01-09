{{  config(
    alias='fundserv_daily_metrics', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

    select * from {{ source('fs_app_tables', 'fundserv_daily_metrics') }}