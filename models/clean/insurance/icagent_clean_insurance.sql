 {{  config(alias='icagent', database='clean', schema='insurance')  }} 
 
Select *

from {{ source ('insurance', 'icagent')  }}