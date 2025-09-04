{{ config(alias='usergroup_vc', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('insurance_curated','usergroup_vc') }}