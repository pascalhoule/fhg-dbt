{{  config(
    alias='ncfmfd', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

select * from {{ source('finance', 'ncfmfd') }}