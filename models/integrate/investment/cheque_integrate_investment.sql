{{  config(
    alias='cheque', 
    database='integrate', 
    schema='investment')  }} 

SELECT * FROM {{ ref('cheque_normalize_investment') }}