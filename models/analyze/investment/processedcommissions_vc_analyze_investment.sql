{{  config(alias='processedcommissions_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('processedcommissions_vc_integrate_investment')  }}