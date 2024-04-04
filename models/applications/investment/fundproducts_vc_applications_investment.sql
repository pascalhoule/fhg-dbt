 {{  config(alias='fundproducts_vc', database='applications', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundproducts_vc_analyze_investment')  }}