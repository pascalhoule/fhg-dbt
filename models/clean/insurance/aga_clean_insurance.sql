{{  config(alias='aga', database='clean', schema='insurance') }} 


SELECT * 
  
  


from {{ source ('insurance', 'aga')  }}