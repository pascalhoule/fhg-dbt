{{  config(alias='ic_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  


from {{ ref ('ic_vc_clean_insurance')  }}