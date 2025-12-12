{{ config(
    alias='empire_fh', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('empire_integrate_comm_extracts') }}

WHERE ("BUSINESS SURNAME" ILIKE '%Horizons%' OR "BUSINESS FIRST NAME" ILIKE '%Horizons%')