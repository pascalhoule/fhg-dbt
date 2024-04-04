{{  config(alias='representatives_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_vc_normalize_investment')  }}