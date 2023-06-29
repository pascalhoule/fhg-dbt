 {{  config(alias='employeerole_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('employeerole_vc_clean_insurance')  }}