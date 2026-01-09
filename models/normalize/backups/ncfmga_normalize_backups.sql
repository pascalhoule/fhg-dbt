{{  config(
    alias='ncfmga', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

select * from {{ source('finance', 'ncfmga') }}