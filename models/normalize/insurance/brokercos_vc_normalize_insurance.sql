{{  config(alias='brokercos_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokercos_vc_clean_insurance')  }}