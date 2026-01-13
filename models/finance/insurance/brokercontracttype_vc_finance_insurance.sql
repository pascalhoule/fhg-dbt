{{ config(
    alias = 'brokercontracttype_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  CODE,
  ENGLISHDESCRIPTION,
  FRENCHDESCRIPTION,
  INDEXSEQUENCE
FROM {{ ref('brokercontracttype_vc_clean_insurance') }}