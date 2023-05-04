 {{  config(alias='brokerphone_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokerphone_vc_clean_insurance')  }}