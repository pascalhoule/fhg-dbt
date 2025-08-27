{{ config(
    alias = 'brokercontractstatus_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
   *
FROM {{ ref('brokercontractstatus_vc_clean_insurance') }}