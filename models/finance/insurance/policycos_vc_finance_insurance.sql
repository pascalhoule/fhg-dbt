{{  config(
    alias='policycos_vc', 
    database='finance', 
    schema='insurance', 
    materialization = "view")  }} 


SELECT * 
  


from {{ ref ('policy_cos_clean_insurance')  }}