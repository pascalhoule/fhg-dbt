{{  config(
    alias='policycos_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 


SELECT * 
  


from {{ ref ('policy_cos_clean_insurance')  }}