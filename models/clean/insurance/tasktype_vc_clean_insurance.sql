{{ config(alias='tasktype_vc', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('insurance_curated','tasktype_vc') }}