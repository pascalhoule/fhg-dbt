{{ config(
    alias='sunlife', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('sunlife_integrate_comm_extracts') }}