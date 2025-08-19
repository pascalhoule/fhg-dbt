{{ config(alias='rbc', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION'], 'select': ['comm_data_user']} 
    ) 
}} 

SELECT * FROM {{ ref('RBC_integrate_comm_extracts') }}