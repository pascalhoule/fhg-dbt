{{ config(
    alias='broker_fh', 
    database='applications', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT *

FROM {{ ref('broker_fh_integrate_insurance') }}
