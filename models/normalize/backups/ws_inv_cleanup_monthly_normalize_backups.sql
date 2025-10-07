{{  config(
    alias='ws_inv_cleanup_monthly', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

    select * from {{ source('fs_app_tables', 'ws_inv_cleanup_monthly') }}