{{ config(alias='mga', database='normalize', schema='insurance') }} 


SELECT *

FROM {{ ref('mga_clean_insurance') }}
