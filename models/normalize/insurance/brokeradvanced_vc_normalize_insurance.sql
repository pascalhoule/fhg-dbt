 {{  config(alias='brokeradvanced_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokeradvanced_vc_clean_insurance')  }}