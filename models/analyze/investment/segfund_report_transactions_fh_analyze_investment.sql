{{  
    config(alias='segfund_report_transactions_fh', 
    database='analyze', 
    schema='investment')  
}} 

SELECT * FROM {{ ref('segfund_report_transactions_fh_integrate_investment') }}