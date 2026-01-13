{{  config(
    alias='advisors', 
    database='normalize', 
    schema='backups', 
    materialized = "table",
    transient = false)  }} 

select * from {{ source('finance', 'advisors') }}