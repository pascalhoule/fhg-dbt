{{  config(alias='registration', database='applications', schema='investment')  }} 


SELECT * 
  


from {{ ref ('registration_analyze_investment')  }}