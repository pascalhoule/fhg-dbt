{{ config(
    alias='equitable', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('equitable_integrate_comm_extracts') }}