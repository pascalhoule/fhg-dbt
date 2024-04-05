 {{  config(alias='fundaccount', database='normalize', schema='investment')  }} 


SELECT *

from {{ ref ('fundaccount_clean_investment')  }}