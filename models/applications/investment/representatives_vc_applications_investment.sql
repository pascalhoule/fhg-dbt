{{  config(alias='representatives_vc', database='applications', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_vc_analyze_investment')  }}