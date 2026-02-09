 {{  config(alias='fund_products', database='normalize', schema='investment')  }} 


SELECT

*

from {{ ref ('fund_products_clean_investment')  }}