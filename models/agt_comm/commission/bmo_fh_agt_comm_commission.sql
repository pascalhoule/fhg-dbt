{{ config(
    alias='bmo_fh', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('bmo_integrate_comm_extracts') }}
WHERE ("BUSINESS SURNAME" ILIKE '%Horizons%' OR "BUSINESS FIRST NAME" ILIKE '%Horizons%')