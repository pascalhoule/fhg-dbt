 {{  config(alias='groups', database='clean', schema='investment')  }} 


SELECT * 
  

from {{ source ('investment', 'groups')  }}