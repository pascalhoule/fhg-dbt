{{  config(alias='policystatus_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policystatus_vc_integrate_insurance')  }}