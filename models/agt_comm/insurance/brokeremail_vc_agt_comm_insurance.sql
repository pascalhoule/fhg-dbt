{{ config(
    alias='brokeremail_vc', 
    database='agt_comm', 
    schema='insurance') }} 

SELECT * 
FROM {{ ref('brokeremail_vc_analyze_insurance') }}