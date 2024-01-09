{{  config(alias='policy_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policy_vc_clean_insurance')  }}