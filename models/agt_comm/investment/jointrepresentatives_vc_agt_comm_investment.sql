{{  config(alias='jointrepresentatives_vc', database='agt_comm', schema='investment')  }} 


SELECT * 
  


from {{ ref ('jointrepresentatives_vc_analyze_investment')  }}