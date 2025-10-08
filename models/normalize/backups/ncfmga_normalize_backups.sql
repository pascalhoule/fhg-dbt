{{  config(
    alias='ncfmga', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

select * from {{ source('finance', 'ncfmga') }}