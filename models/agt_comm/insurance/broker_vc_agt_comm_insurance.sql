{{  config(alias='broker_vc', database='agt_comm', schema='insurance', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('broker_vc_analyze_insurance')  }}