{{ config(alias='rbc', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view"
    ) 
}} 

SELECT * FROM {{ ref('RBC_integrate_comm_extracts') }}