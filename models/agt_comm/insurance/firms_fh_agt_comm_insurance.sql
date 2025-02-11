{{ config(
    alias='firms_fh', 
    database='agt_comm', 
    schema='insurance') }} 

SELECT *

FROM {{ ref('firms_fh_analyze_insurance') }}