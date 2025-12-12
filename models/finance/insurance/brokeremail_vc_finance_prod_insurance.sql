{{ config(
    alias = 'brokeremail_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  AGENTCODE, 
  TYPE, 
  EMAILADDRESS, 
  CASLAPPROVED
FROM {{ ref('brokeremail_vc_clean_insurance') }}