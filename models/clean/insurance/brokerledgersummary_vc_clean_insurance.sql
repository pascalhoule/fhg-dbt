{{ config(alias='brokerledgersummary_vc', database='clean', schema='insurance') }} 


SELECT *

FROM {{ source ('insurance_curated', 'brokerledgersummary_vc') }}
