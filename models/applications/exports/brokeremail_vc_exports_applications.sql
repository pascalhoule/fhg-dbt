{{ config(
    alias='brokeremail_vc', 
    database='applications', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT *

FROM {{ ref('brokeremail_vc_integrate_insurance') }}
