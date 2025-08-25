{{ config(alias='bmo', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view"
)
}} 

SELECT * FROM {{ ref('bmo_integrate_comm_extracts') }}