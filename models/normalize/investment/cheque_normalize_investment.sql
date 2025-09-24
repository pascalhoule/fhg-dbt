 {{  config(
    alias='cheque', 
    database='normalize', 
    schema='investment')  }} 

SELECT * FROM {{ ref('cheque_clean_investment') }}