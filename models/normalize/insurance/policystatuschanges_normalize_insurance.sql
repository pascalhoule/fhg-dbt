{{  config(alias='policystatuschanges', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policystatuschanges_clean_insurance')  }}