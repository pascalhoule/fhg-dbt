 {{  config(alias='brokeremail_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('brokeremail_vc_clean_insurance')  }}