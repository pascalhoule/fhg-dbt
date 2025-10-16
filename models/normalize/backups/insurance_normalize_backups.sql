{{  config(
    alias='insurance', 
    database='normalize', 
    schema='backups', 
    materialized = "table")  }} 

select * from {{ source('finance', 'insurance') }}