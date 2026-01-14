{{  
    config(alias='segfund_report_transactions_fh', 
    database='finance', 
    schema='investment')  
}} 

SELECT * FROM {{ ref('segfund_report_transactions_fh_normalize_investment') }}