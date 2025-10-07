{{  config(
    alias='fundserv_daily_metrics', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

    select * from {{ source('app_tables', 'fundserv_daily_metrics') }}