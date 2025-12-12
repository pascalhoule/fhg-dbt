{{ config(
    alias='canadalife', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('canadalife_integrate_comm_extracts') }}