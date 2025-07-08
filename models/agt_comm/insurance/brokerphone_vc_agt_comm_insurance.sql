{{ config(
    alias='brokerphone_vc', 
    database='agt_comm', 
    schema='insurance') }} 

SELECT *
FROM {{ ref('brokerphone_vc_analyze_insurance') }}