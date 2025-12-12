{{ config(
    alias='manulife', 
    database='agt_comm', 
    schema='commission', 
    materialized="view" 
) }} 

SELECT * FROM {{ ref('manulife_integrate_comm_extracts') }}