 {{  config(alias='hierarchy_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('hierarchy_vc_clean_insurance')  }}