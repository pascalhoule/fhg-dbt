{{  config(alias='planmodalfactor_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  


from {{ ref ('planmodalfactor_vc_clean_insurance')  }}