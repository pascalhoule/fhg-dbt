{{  config(alias='brokeremail_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokeremail_vc_integrate_insurance') }}