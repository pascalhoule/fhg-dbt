{{  config(alias='brokercontractprovince_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokercontractprovince_vc_integrate_insurance')  }}