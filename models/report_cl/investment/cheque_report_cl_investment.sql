{{  config(
    alias='cheque', 
    database='report_cl', 
    schema='investment')  }} 

SELECT * FROM {{ ref('cheque_analyze_investment') }}