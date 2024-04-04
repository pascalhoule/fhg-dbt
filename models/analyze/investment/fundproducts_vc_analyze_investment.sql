{{  config(alias='fundproducts_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundproducts_vc_integrate_investment')  }}