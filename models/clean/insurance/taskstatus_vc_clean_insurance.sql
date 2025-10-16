{{ config(alias='taskstatus_vc', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('insurance_curated','taskstatus_vc') }}