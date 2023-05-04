 {{  config(alias='brokertags_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokertags_vc_clean_insurance')  }}