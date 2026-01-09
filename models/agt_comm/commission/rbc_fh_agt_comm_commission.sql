{{ config(
    alias='rbc_fh', 
    database='agt_comm', 
    schema='commission', 
    materialized="view"
) }} 

SELECT * FROM {{ ref('RBC_integrate_comm_extracts') }}

WHERE ("BUSINESS SURNAME" ILIKE '%Horizons%')