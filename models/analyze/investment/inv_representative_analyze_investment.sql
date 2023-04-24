 {{  config(alias='inv_representative', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('inv_representative_integrate_investment')  }}
