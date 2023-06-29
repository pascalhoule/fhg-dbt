 {{  config(alias='cosrole_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('cosrole_vc_clean_insurance')  }}