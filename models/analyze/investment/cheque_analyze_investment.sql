{{  config(
    alias='cheque', 
    database='analyze', 
    schema='investment')  }} 

SELECT * FROM {{ ref('cheque_integrate_investment') }}