{{  config(alias='policystatus_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  


from {{ ref ('policystatus_vc_clean_insurance')  }}