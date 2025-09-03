{{ config(alias='tasks_vc', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('insurance_curated','tasks_vc') }}