{{  config(
    alias='infovr', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

select * from {{ source('finance', 'infovr') }}