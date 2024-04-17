{{  config(alias='brokertags_vc', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('brokertags_vc_normalize_insurance') }}