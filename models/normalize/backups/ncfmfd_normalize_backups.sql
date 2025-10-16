{{  config(
    alias='ncfmfd', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

select * from {{ source('finance', 'ncfmfd') }}