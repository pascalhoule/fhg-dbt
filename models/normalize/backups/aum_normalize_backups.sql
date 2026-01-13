{{  config(
    alias='aum', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

select * from {{ source('finance', 'aum') }}