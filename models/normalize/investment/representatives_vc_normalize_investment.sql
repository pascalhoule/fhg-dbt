 {{  config(alias='representatives_vc', database='normalize', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_vc_clean_investment')  }}