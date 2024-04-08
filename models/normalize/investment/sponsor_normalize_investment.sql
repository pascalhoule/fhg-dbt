 {{  config(alias='sponsor', database='normalize', schema='investment')  }} 


SELECT

*

from {{ ref ('sponsor_clean_investment')  }}