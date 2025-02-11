{{ config(
    alias='hierarchy_fh', 
    database='agt_comm', 
    schema='insurance') }} 

SELECT *

FROM {{ ref('hierarchy_fh_analyze_insurance') }}