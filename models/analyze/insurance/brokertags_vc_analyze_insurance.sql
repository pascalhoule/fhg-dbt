{{  config(alias='brokertags_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokertags_vc_integrate_insurance')  }}