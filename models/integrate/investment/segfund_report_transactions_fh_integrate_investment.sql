{{  
    config(alias='segfund_report_transactions_fh', 
    database='integrate', 
    schema='investment')  
}} 

SELECT * FROM {{ ref('segfund_report_transactions_fh') }}