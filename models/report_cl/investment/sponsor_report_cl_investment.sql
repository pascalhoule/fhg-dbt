{{  config(alias='sponsor', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsor_analyze_investment')  }}