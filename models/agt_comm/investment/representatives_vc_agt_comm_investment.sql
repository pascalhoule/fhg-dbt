{{  config(alias='representatives_vc', database='agt_comm', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_vc_analyze_investment')  }}