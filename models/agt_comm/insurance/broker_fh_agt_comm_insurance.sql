{{  config(alias='broker_fh', database='agt_comm', schema='insurance', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('broker_fh_analyze_insurance')  }}