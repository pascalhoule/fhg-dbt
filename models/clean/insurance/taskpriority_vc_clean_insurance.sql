{{ config(alias='taskpriority_vc', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('insurance_curated','taskpriority_vc') }}