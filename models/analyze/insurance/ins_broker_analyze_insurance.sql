 {{  config(alias='ins_broker', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('ins_broker_integrate_insurance')  }}