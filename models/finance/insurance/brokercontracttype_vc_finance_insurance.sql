{{ config(
    alias = 'brokercontracttype_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  CODE,
  ENGLISHDESCRIPTION,
  FRENCHDESCRIPTION,
  INDEXSEQUENCE
FROM {{ ref('brokercontracttype_vc_clean_insurance') }}