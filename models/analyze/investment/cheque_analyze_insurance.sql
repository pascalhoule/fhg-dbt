{{  config(alias='cheque', database='analyze', schema='insurance')  }}

SELECT * 
  
from {{ ref ('cheque_integrate_insurance')  }}