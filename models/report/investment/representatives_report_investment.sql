{{  config(alias='representatives', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_analyze_investment')  }}