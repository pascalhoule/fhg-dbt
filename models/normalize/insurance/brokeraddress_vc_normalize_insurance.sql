 {{  config(alias='brokeraddress_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokeraddress_vc_clean_insurance')  }}