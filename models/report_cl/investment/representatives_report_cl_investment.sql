{{  config(alias='representatives', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_analyze_investment')  }}