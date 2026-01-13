{{  config(
    alias='mfd', 
    database='normalize', 
    schema='backups', 
    materialized = "table", 
    transient = false)  }} 

select * from {{ source('finance', 'mfd') }}