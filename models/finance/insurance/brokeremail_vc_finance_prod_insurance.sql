{{ config(
    alias = 'brokeremail_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  AGENTCODE, 
  TYPE, 
  EMAILADDRESS, 
  CASLAPPROVED
FROM {{ ref('brokeremail_vc_clean_insurance') }}