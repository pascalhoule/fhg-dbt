 {{  config(alias='representativeaddress_vc', database='normalize', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representativeaddress_vc_clean_investment')  }}