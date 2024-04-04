{{  config(alias='sponsors_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsors_vc_integrate_investment')  }}