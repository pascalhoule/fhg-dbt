{{ config(alias='mga', database='agt_comm', schema='insurance') }} 


SELECT *

FROM {{ ref('mga_analyze_insurance') }}