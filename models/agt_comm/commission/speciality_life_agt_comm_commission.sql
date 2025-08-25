{{ config(alias='speciality_life', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view" 
)
}} 

SELECT * FROM {{ ref('speciality_life_integrate_comm_extracts') }}