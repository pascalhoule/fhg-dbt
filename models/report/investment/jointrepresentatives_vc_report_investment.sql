{{  config(alias='jointrepresentatives_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('jointrepresentatives_vc_analyze_investment')  }}