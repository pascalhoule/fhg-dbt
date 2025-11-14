 {{  config(alias='processedcommissions_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('processedcommissions_vc_normalize_investment')  }}