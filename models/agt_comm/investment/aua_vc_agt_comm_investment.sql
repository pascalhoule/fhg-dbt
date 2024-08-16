 {{  config(alias='aua_vc', database='agt_comm', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_analyze_investment')  }}