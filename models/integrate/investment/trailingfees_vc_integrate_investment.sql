{{  config(alias='trailingfees_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('trailingfees_vc_normalize_investment')  }}