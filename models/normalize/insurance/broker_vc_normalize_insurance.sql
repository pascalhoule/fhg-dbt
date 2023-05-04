 {{  config(alias='broker_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('broker_vc_clean_insurance')  }}