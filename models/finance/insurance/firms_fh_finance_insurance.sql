{{ config(
    alias='firms_fh', 
    database='finance', 
    schema='insurance') }} 

SELECT *

FROM {{ ref('firms_fh_analyze_insurance') }}