{{  config(alias='policy', database='finance', schema='insurance', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('policy_clean_insurance')  }}