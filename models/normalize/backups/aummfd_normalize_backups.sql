{{  config(
    alias='aummfd', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

select * from {{ source('finance', 'aummfd') }}