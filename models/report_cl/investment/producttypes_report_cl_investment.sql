{{  config(alias='producttypes', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('producttypes_analyze_investment')  }}