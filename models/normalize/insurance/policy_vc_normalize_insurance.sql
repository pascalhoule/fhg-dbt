{{  config(alias='policy_vc', database='normalize', schema='insurance', tags = ["policy_fh"])  }} 


SELECT * 
  


from {{ ref ('policy_vc_clean_insurance')  }}