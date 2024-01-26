 {{  config(alias='brokercos_vc', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('brokercos_vc_normalize_insurance')  }}