{{ config(alias='rider_vc', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('insurance_curated','rider_vc') }}