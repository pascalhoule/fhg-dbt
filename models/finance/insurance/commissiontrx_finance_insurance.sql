{{  config(alias='commissiontrx', database='finance', schema='insurance') }} 


SELECT * 
 
  
from {{ ref('commissiontrx_clean_insurance') }}