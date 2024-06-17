{{  config(alias='broker_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  


from {{ ref ('broker_vc_clean_insurance')  }}