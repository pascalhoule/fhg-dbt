{{  config(alias='brokeradvanced_vc', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('brokeradvanced_vc_normalize_insurance')  }}