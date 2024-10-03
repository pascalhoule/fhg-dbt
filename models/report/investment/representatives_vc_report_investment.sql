{{  config(alias='representatives_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_vc_analyze_investment')  }}