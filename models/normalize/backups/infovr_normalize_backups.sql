{{  config(
    alias='infovr', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

select * from {{ source('finance', 'infovr') }}