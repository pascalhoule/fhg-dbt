{{ config(alias='mga', database='integrate', schema='insurance') }} 


SELECT *

FROM {{ ref('mga_normalize_insurance') }}