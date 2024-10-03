{{  config(alias='sponsor', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsor_analyze_investment')  }}