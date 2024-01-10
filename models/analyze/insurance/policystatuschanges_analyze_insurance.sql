{{  config(alias='policystatuschanges', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policystatuschanges_integrate_insurance')  }}