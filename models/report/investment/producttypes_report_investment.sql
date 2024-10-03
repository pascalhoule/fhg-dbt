{{  config(alias='producttypes', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('producttypes_analyze_investment')  }}