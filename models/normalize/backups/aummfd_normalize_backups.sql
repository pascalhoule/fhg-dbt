{{  config(
    alias='aummfd', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

select * from {{ source('finance', 'aummfd') }}