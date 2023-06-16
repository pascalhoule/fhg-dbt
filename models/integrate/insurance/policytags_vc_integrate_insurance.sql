{{  config(alias='policytags_vc', database='integrate', schema='insurance')  }} 
 


SELECT *
  


from {{ ref ('policytags_vc_normalize_insurance')  }}