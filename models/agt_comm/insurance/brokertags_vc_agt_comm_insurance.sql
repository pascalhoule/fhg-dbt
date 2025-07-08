{{ config(
    alias='brokertags_vc', 
    database='agt_comm', 
    schema='insurance') }} 

SELECT *
FROM {{ ref('brokertags_vc_analyze_insurance') }}