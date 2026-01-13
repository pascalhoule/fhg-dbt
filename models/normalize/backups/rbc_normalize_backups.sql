{{  config(
    alias='rbc', 
    database='normalize', 
    schema='backups', 
    materialized = "table", 
    transient = false)  }} 

select * from {{ source('finance', 'rbc') }}