{{  config(alias='commission_vc', database='agt_comm', schema='insurance', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('commission_vc_analyze_insurance')  }}