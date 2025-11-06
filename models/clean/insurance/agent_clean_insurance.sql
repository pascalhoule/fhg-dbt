{{  config(alias='agent', database='clean', schema='insurance') }} 


SELECT * 
  
  


from {{ source ('insurance', 'agent')  }}