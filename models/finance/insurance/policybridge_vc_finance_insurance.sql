{{  config(
    alias='policybridge_vc', 
    database='finance', 
    schema='insurance', 
    materialization = "view")  }} 


SELECT * 
  


from {{ ref ('policybridge_vc_clean_insurance')  }}