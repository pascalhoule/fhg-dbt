{{  config(
    alias='mfd', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

select * from {{ source('finance', 'mfd') }}