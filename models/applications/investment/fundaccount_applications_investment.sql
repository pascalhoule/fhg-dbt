{{  config(alias='fundaccount', database='applications', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_analyze_investment')  }}