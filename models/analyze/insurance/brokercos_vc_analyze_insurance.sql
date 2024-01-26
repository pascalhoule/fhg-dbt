{{  config(alias='brokercos_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokercos_vc_integrate_insurance')  }}