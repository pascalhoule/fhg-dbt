{{ config(alias='mga', database='analyze', schema='insurance') }} 


SELECT *

FROM {{ ref('mga_integrate_insurance') }}