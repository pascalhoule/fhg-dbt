{{  config(alias='policystatus_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  


from {{ ref ('policystatus_vc_clean_insurance')  }}