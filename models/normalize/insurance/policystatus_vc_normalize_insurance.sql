 {{  config(alias='policystatus_vc', database='normalize', schema='insurance', tags=["policy_fh"])  }} 


SELECT * 
  


from {{ ref ('policystatus_vc_clean_insurance')  }}