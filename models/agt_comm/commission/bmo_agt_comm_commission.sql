{{ config(
    alias='bmo', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('bmo_integrate_comm_extracts') }}