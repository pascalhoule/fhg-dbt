 {{  config(alias='policystatus_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policystatus_vc_clean_insurance')  }}