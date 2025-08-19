{{ config(alias='rbc', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION']} 
    ) 
}} 

SELECT * FROM {{ ref('RBC_integrate_comm_extracts') }}