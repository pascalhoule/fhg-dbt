{{  config(alias='cheque', database='clean', schema='investment')  }} 


SELECT * 

from {{ source ('investment', 'cheque')  }} 