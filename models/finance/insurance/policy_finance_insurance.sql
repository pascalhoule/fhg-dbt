{{  config(alias='policy', database='finance', schema='insurance', materialization = "view")  }} 


SELECT * 
  


from {{ ref ('policy_clean_insurance')  }}