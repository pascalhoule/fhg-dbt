{{  config(alias='fundproducts_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundproducts_vc_analyze_investment')  }}