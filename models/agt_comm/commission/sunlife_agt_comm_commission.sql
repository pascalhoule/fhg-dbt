{{ config(alias='sunlife', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION']},)  
}} 

SELECT * FROM {{ ref('sunlife_integrate_comm_extracts') }}